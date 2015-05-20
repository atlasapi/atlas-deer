package org.atlasapi.system.debug;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.system.bootstrap.workers.ExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacySegmentMigrator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

@Controller
public class ContentDebugController {

    private final NumberToShortStringCodec uppercase = new SubstitutionTableNumberCodec();
    private final NumberToShortStringCodec lowercase = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class,
            new JsonSerializer<DateTime>() {

                @Override
                public JsonElement serialize(DateTime src, Type typeOfSrc,
                        JsonSerializationContext context) {
                    return new JsonPrimitive(src.toString());
                }
            }).create();

    private static final Function<Content, ResourceRef> TO_RESOURCE_REF = new Function<Content, ResourceRef>() {

        @Override
        public ResourceRef apply(Content input) {
            return input.toRef();
        }
    };
    private static final Function<ResourceRef, Publisher> TO_SOURCE = new Function<ResourceRef, Publisher>() {

        @Override
        public Publisher apply(ResourceRef input) {
            return input.getPublisher();
        }
    };

    private final Logger log = LoggerFactory.getLogger(ContentDebugController.class);
    private final ContentResolver legacyResolver;
    private final EquivalentContentStore equivalentContentStore;
    private final ContentStore contentStore;
    private final ExplicitEquivalenceMigrator explicitEquivalenceMigrator;
    private final LegacySegmentMigrator segmentMigrator;

    public ContentDebugController(ContentResolver legacyResolver,
            EquivalentContentStore equivalentContentStore, ContentStore contentStore,
            ExplicitEquivalenceMigrator explicitEquivalenceMigrator, LegacySegmentMigrator segmentMigrator) {
        this.explicitEquivalenceMigrator = checkNotNull(explicitEquivalenceMigrator);
        this.legacyResolver = checkNotNull(legacyResolver);
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.contentStore = checkNotNull(contentStore);
        this.segmentMigrator = checkNotNull(segmentMigrator);
    }

    @RequestMapping("/system/id/decode/uppercase/{id}")
    private void decodeUppercaseId(@PathVariable("id") String id,
            final HttpServletResponse response) throws IOException {
        response.getWriter().write(uppercase.decode(id).toString());
    }

    @RequestMapping("/system/id/decode/lowercase/{id}")
    private void decodeLowercaseId(@PathVariable("id") String id,
            final HttpServletResponse response) throws IOException {
        response.getWriter().write(lowercase.decode(id).toString());
    }

    @RequestMapping("/system/id/encode/uppercase/{id}")
    private void encodeUppercaseId(@PathVariable("id") Long id, final HttpServletResponse response)
            throws IOException {
        response.getWriter().write(uppercase.encode(BigInteger.valueOf(id)));
    }

    @RequestMapping("/system/id/encode/lowercase/{id}")
    private void encodeLowercaseId(@PathVariable("id") Long id, final HttpServletResponse response)
            throws IOException {
        response.getWriter().write(lowercase.encode(BigInteger.valueOf(id)));
    }

    @RequestMapping("/system/debug/content/{id}")
    public void printContent(@PathVariable("id") Long id, final HttpServletResponse response) throws Exception {
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(id));
        Resolved<Content> result = Futures.get(contentStore.resolveIds(ids), 1, TimeUnit.MINUTES, Exception.class);
        Content content = result.getResources().first().orNull();
        gson.toJson(content, response.getWriter());
    }

    @RequestMapping("/system/debug/content/{id}/migrate")
    public void forceEquivUpdate(@PathVariable("id") String id,
            @RequestParam(value = "publisher", required = true) String publisherKey,
            final HttpServletResponse response) throws IOException {
        try {
            Long decodedId = lowercase.decode(id).longValue();

            StringBuilder respString = new StringBuilder();
            Maybe<Publisher> publisherMaybe = Publisher.fromKey(publisherKey);
            if (publisherMaybe.isNothing()) {
                response.setStatus(400);
                response.getWriter().write("Supply a valid publisher key");
                return;
            }

            Content content = resolveLegacyContent(decodedId);
            if (content instanceof Item) {
                Item item = (Item) content;
                List<SegmentEvent> segmentEvents = item.getSegmentEvents();
                if (!segmentEvents.isEmpty()) {
                    for (SegmentEvent segmentEvent : segmentEvents) {
                        segmentMigrator.migrateLegacySegment(segmentEvent.getSegmentRef().getId());
                        log.info("Migrated segment " + segmentEvent.getSegmentRef().getId());
                    }
                }
            }

            respString.append("Resolved legacy content ").append(content.getId());
            WriteResult<Content, Content> writeResult = contentStore.writeContent(content);

            if (!writeResult.written()) {
                response.getWriter().write(respString.append("\nNo write occured when migrating content into C* store").toString());
                response.setStatus(500);
                return;
            }
            respString.append("\nMigrated content into C* content store");
            Optional<EquivalenceGraphUpdate> graphUpdate =
                    explicitEquivalenceMigrator.migrateEquivalence(content);
            equivalentContentStore.updateContent(content.toRef());
            respString.append("\nEquivalent content store updated using content ref");

            response.setStatus(200);
            response.getWriter().write(respString.toString());
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }

    private Content resolveLegacyContent(Long id) {
        return Iterables.getOnlyElement(Futures.getUnchecked(legacyResolver.resolveIds(
                ImmutableList.of(Id.valueOf(id)))).getResources());
    }
}
