package org.atlasapi.system.debug;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EsContent;
import org.atlasapi.content.EsContentTranslator;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.system.bootstrap.ContentBootstrapListener;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacySegmentMigrator;
import org.atlasapi.util.EsObject;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ContentDebugController {

    private final NumberToShortStringCodec uppercase = new SubstitutionTableNumberCodec();
    private final NumberToShortStringCodec lowercase = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final Gson gson = new GsonBuilder().registerTypeAdapter(
            DateTime.class,
            (JsonSerializer<DateTime>) (src, typeOfSrc, context) -> new JsonPrimitive(src.toString())
    )
            .create();
    private final ObjectMapper jackson = new ObjectMapper();

    private final ContentResolver legacyContentResolver;
    private final AtlasPersistenceModule persistence;
    private final ContentIndex index;
    private final EsContentTranslator esContentTranslator;

    private final ContentBootstrapListener contentBootstrapListener;
    private final ContentBootstrapListener contentAndEquivalentsBootstrapListener;

    public ContentDebugController(
            ContentResolver legacyContentResolver,
            LegacySegmentMigrator legacySegmentMigrator,
            AtlasPersistenceModule persistence,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator,
            ContentIndex index,
            EsContentTranslator esContentTranslator) {
        this.legacyContentResolver = checkNotNull(legacyContentResolver);
        this.persistence = checkNotNull(persistence);
        this.index = checkNotNull(index);
        this.esContentTranslator = checkNotNull(esContentTranslator);

        this.contentBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(persistence.nullMessageSendingContentStore())
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withContentIndex(index)
                .withMigrateHierarchies(legacySegmentMigrator, legacyContentResolver)
                .build();

        this.contentAndEquivalentsBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(persistence.nullMessageSendingContentStore())
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withContentIndex(index)
                .withMigrateHierarchies(legacySegmentMigrator, legacyContentResolver)
                .withMigrateEquivalents(persistence.nullMessageSendingEquivalenceGraphStore())
                .build();
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

    @RequestMapping("/system/id/toLowercase/{id}")
    private void toLowercaseId(@PathVariable("id") String id, final HttpServletResponse response)
            throws IOException {
        response.getWriter().write(lowercase.encode(uppercase.decode(id)));
    }

    @RequestMapping("/system/id/toUppercase/{id}")
    private void toUppercaseId(@PathVariable("id") String id, final HttpServletResponse response)
            throws IOException {
        response.getWriter().write(uppercase.encode(lowercase.decode(id)));
    }

    /* Deactivates a piece of content by setting activelyPublished to false */
    @RequestMapping("/system/debug/content/{id}/deactivate")
    private void deactivateContent(@PathVariable("id") String id,
            final HttpServletResponse response) throws IOException {
        try {
            ContentStore contentStore = persistence.contentStore();
            Resolved<Content> resolved = Futures.get(
                    contentStore.resolveIds(ImmutableList.of(Id.valueOf(lowercase.decode(id)))),
                    IOException.class
            );
            Content content = Iterables.getOnlyElement(resolved.getResources());
            content.setActivelyPublished(false);
            WriteResult<Content, Content> writeResult = contentStore.writeContent(content);
            gson.toJson(writeResult.written(), response.getWriter());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /* Updates the equivalent content store representation of a piece of content */
    @RequestMapping("/system/debug/content/{id}/updateEquivalentContentStore")
    private void updateEquivalentContentStore(@PathVariable("id") String id,
            final HttpServletResponse response) throws IOException {
        try {
            Id contentId = Id.valueOf(lowercase.decode(id));
            persistence.getEquivalentContentStore().updateContent(contentId);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /* Returns the JSON representation of a legacy content read from Mongo and translated to the v4 model */
    @RequestMapping("/system/debug/content/{id}/legacy")
    public void printLegacyContent(@PathVariable("id") String id,
            final HttpServletResponse response) throws Exception {
        ListenableFuture<Resolved<Content>> resolving = legacyContentResolver
                .resolveIds(ImmutableList.of(Id.valueOf(lowercase.decode(id))));
        Resolved<Content> resolved = Futures.get(resolving, Exception.class);
        Content content = Iterables.getOnlyElement(resolved.getResources());
        gson.toJson(content, response.getWriter());
    }

    /* Returns the JSON representation of a piece of content stored in the Cassandra store */
    @RequestMapping("/system/debug/content/{id}")
    public void printContent(@PathVariable("id") String id, final HttpServletResponse response)
            throws Exception {
        Long decodedId = lowercase.decode(id).longValue();
        ImmutableList<Id> ids = ImmutableList.of(Id.valueOf(decodedId));
        Resolved<Content> result = Futures.get(
                persistence.contentStore().resolveIds(ids), 1, TimeUnit.MINUTES, Exception.class
        );
        Content content = result.getResources().first().orNull();
        gson.toJson(content, response.getWriter());
    }

    /* Returns the JSON representation of a piece of content stored in the equivalent content store */
    @RequestMapping("/system/debug/equivalentcontent/{id}")
    public void printEquivalentContent(@PathVariable("id") String idString,
            HttpServletResponse response) throws Exception {
        Id id = Id.valueOf(lowercase.decode(idString).longValue());
        ImmutableList<Id> ids = ImmutableList.of(id);
        ResolvedEquivalents<Content> result = Futures.get(
                persistence.getEquivalentContentStore().resolveIds(
                        ids, Publisher.all(), ImmutableSet.of()
                ),
                1,
                TimeUnit.MINUTES,
                Exception.class
        );
        Content content = result.get(id).iterator().next();
        gson.toJson(content, response.getWriter());
    }

    /* Returns the JSON representation of a set stored in the equivalent content store */
    @RequestMapping("/system/debug/equivalentcontent/{id}/set")
    public void printEquivalentContentSet(@PathVariable("id") String idString,
            HttpServletResponse response) throws Exception {
        Id id = Id.valueOf(lowercase.decode(idString).longValue());
        ImmutableList<Id> ids = ImmutableList.of(id);
        ResolvedEquivalents<Content> result = Futures.get(
                persistence.getEquivalentContentStore().resolveIds(
                        ids, Publisher.all(), ImmutableSet.of()
                ),
                1,
                TimeUnit.MINUTES,
                Exception.class
        );
        ImmutableSet<Content> content = result.get(id);
        gson.toJson(content, response.getWriter());
    }

    /* Returns the JSON representation of a piece of equivalent content graph */
    @RequestMapping("/system/debug/content/{id}/graph")
    public void printContentEquivalence(@PathVariable("id") String id,
            final HttpServletResponse response)
            throws Exception {
        Id decodedId = Id.valueOf(lowercase.decode(id));
        ImmutableList<Id> ids = ImmutableList.of(decodedId);
        OptionalMap<Id, EquivalenceGraph> equivalenceGraph = Futures.get(
                persistence.getContentEquivalenceGraphStore().resolveIds(ids),
                1,
                TimeUnit.MINUTES,
                Exception.class
        );
        if (equivalenceGraph.get(decodedId).isPresent()) {
            gson.toJson(equivalenceGraph.get(decodedId).get(), response.getWriter());
        } else {
            gson.toJson(
                    String.format(
                            "Equivalence graph for %s not found",
                            id
                    ),
                    response.getWriter()
            );
        }
    }

    @RequestMapping("/system/debug/content/{id}/index")
    public void indexContent(@PathVariable("id") String id, final HttpServletResponse response)
            throws IOException {
        try {
            Id decodedId = Id.valueOf(lowercase.decode(id).longValue());
            Resolved<Content> resolved =
                    Futures.get(
                            persistence.contentStore().resolveIds(ImmutableList.of(decodedId)),
                            IOException.class
                    );
            index.index(resolved.getResources().first().get());
            Content content = resolved.getResources().first().get();
            if (content instanceof Container) {
                EsContent esContainer = esContentTranslator.toEsContainer((Container) content);
                Map<String, Object> map = EsObject.TO_MAP.apply(esContainer);
                jackson.writeValue(response.getWriter(), map);
            } else {
                EsContent esContent = esContentTranslator.toEsContent((Item) content);
                Map<String, Object> map = EsObject.TO_MAP.apply(esContent);
                jackson.writeValue(response.getWriter(), map);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping("/system/debug/content/{id}/migrate")
    public void forceEquivUpdate(@PathVariable("id") String id,
            @RequestParam(name = "equivalents", defaultValue = "false") Boolean migrateEquivalents,
            final HttpServletResponse response) throws IOException {
        try {
            Long decodedId = lowercase.decode(id).longValue();

            Content content = resolveLegacyContent(decodedId);

            ContentBootstrapListener listener = migrateEquivalents
                                                ? contentAndEquivalentsBootstrapListener
                                                : contentBootstrapListener;

            ContentBootstrapListener.Result result = content.accept(listener);

            response.setStatus(HttpStatus.OK.value());
            response.getWriter().println(result.toString());
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }



    private Content resolveLegacyContent(Long id) {
        return Iterables.getOnlyElement(
                Futures.getUnchecked(legacyContentResolver
                        .resolveIds(ImmutableList.of(Id.valueOf(id)))).getResources()
        );
    }

}
