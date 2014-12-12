package org.atlasapi.system.debug;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

@Controller
public class ContentDebugController {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new JsonSerializer<DateTime>() {
        @Override
        public JsonElement serialize(DateTime src, Type typeOfSrc, JsonSerializationContext context) {
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
    private final EquivalenceGraphStore graphStore;
    private final ContentStore contentStore;
    private final LookupEntryStore entryStore;

    public ContentDebugController(ContentResolver legacyResolver, EquivalentContentStore equivalentContentStore,
                                  EquivalenceGraphStore graphStore, ContentStore contentStore,
                                  LookupEntryStore entryStore) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.graphStore = checkNotNull(graphStore);
        this.contentStore = checkNotNull(contentStore);
        this.entryStore = checkNotNull(entryStore);
    }

    @RequestMapping("/system/debug/content/{id}")
    public void printContent(@PathVariable("id") Long id, final HttpServletResponse response) {
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(id));
        Futures.addCallback(contentStore.resolveIds(ids), new FutureCallback<Resolved<Content>>() {

            @Override
            public void onSuccess(Resolved<Content> result) {
                try {
                    Content content = result.getResources().first().orNull();
                    gson.toJson(content, response.getWriter());
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    t.printStackTrace(response.getWriter());
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
        });
    }

    @RequestMapping("/system/debug/content/{id}/migrate")
    public void forceEquivUpdate(@PathVariable("id") Long id, @RequestParam(value = "publisher", required = true) String publisherKey,
                                 final HttpServletResponse response) throws IOException {
        try {
            Maybe<Publisher> publisherMaybe = Publisher.fromKey(publisherKey);
            if (publisherMaybe.isNothing()) {
                response.setStatus(400);
                response.getWriter().write("Supply a valid publisher key");
                return;
            }

            Resolved<Content> resolved = Futures.getUnchecked(legacyResolver.resolveIds(ImmutableList.of(Id.valueOf(id))));
            WriteResult<Content, Content> writeResult = contentStore.writeContent(resolved.getResources().first().get());

            if (!writeResult.written()) {
                response.getWriter().write("No write occured when migrating content into C* store");
                response.setStatus(500);
                return;
            }

            Optional<EquivalenceGraphUpdate> graphUpdate = migrateExplicitEquivalence(writeResult.getResource());
            equivalentContentStore.updateEquivalences(graphUpdate.get());

            response.setStatus(200);
            response.getWriter().write("Migrated content " + resolved.getResources().first().get().getId().toString());
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }

    private Optional<EquivalenceGraphUpdate> migrateExplicitEquivalence(Content content) {
        long id = content.getId().longValue();
        try {
            LookupEntry entry = Iterables.getOnlyElement(entryStore.entriesForIds(ImmutableList.of(id)));
            if (entry.explicitEquivalents() == null || entry.explicitEquivalents().isEmpty()) {
                throw new IllegalArgumentException("Content " + id + " has no explicit equivalents");
            } else {
                return updateEquivalences(content, entry);
            }
        } catch (WriteException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private Optional<EquivalenceGraphUpdate> updateEquivalences(Content content, LookupEntry entry) throws WriteException, ExecutionException {
        ImmutableSet<ResourceRef> refs = resolveEquivRefsToResourceRefs(entry.explicitEquivalents());
        log.info("Resolved {}/{} equivalent refs for content {}",
                refs.size(), entry.explicitEquivalents().size(), content.getId());
        ImmutableSet<Publisher> sources = FluentIterable.from(refs).transform(TO_SOURCE).toSet();
        Optional<EquivalenceGraphUpdate> update = graphStore.updateEquivalences(content.toRef(), refs, sources);
        return update;
    }

    private ImmutableSet<ResourceRef> resolveEquivRefsToResourceRefs(Set<LookupRef> lookupRefs) throws ExecutionException {
        ImmutableSet<Id> ids = FluentIterable.from(lookupRefs).transform(LookupRef.TO_ID).transform(Id.fromLongValue()).toSet();
        Resolved<Content> contentResolved = Futures.get(legacyResolver.resolveIds(ids), ExecutionException.class);
        return contentResolved.getResources().transform(TO_RESOURCE_REF).toSet();
    }
}
