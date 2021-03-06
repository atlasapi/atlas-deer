package org.atlasapi.system.debug;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.system.bootstrap.ContentBootstrapListener;
import org.atlasapi.system.bootstrap.ContentNeo4jMigrator;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.system.legacy.LegacySegmentMigrator;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ContentDebugController {

    public static final Pattern ILLEGAL_URI_CHARACTERS = Pattern.compile("[$\"{}\\\\;]");
    private final NumberToShortStringCodec uppercase = new SubstitutionTableNumberCodec();
    private final NumberToShortStringCodec lowercase = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Splitter commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    private final Gson gson;
    private final ObjectMapper jackson;

    private final LegacyContentResolver legacyContentResolver;
    private final ContentStore contentStore;
    private final EquivalenceGraphStore equivalenceGraphStore;
    private final EquivalentContentStore equivalentContentStore;
    private final Neo4jContentStore neo4jContentStore;
    private final ContentStore nullMessageSendingContentStore;
    private final EquivalentContentStore nullMessageSendingEquivalentContentStore;

    private final ContentBootstrapListener contentBootstrapListener;
    private final ContentBootstrapListener nullMessageSendingContentBootstrapListener;
    private final ContentBootstrapListener contentAndHierarchyBootstrapListener;
    private final ContentBootstrapListener nullMessageSendingContentAndHierarchyBootstrapListener;
    private final ContentBootstrapListener contentAndEquivBootstrapListener;
    private final ContentBootstrapListener nullMessageSendingContentAndEquivBootstrapListener;
    private final ContentBootstrapListener contentEquivAndHierarchyBootstrapListener;
    private final ContentBootstrapListener nullMessageSendingContentEquivAndHierarchyBootstrapListener;
    private final ContentBootstrapListener forceWriteBootstrapListener;
    private final ContentNeo4jMigrator contentNeo4jMigrator;

    private ContentDebugController(Builder builder) {
        gson = new GsonBuilder()
                .registerTypeAdapter(
                        DateTime.class,
                        (JsonSerializer<DateTime>) (src, typeOfSrc, context) ->
                                new JsonPrimitive(src.toString())
                )
                .registerTypeAdapter(
                        Interval.class,
                        (JsonSerializer<Interval>) (src, typeOfSrc, context) ->
                                new JsonPrimitive(src.toString())
                )
                .create();

        jackson = new ObjectMapper()
                .registerModule(new GuavaModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JodaModule())
                .configure(WRITE_DATES_AS_TIMESTAMPS , false)
                .configure(FAIL_ON_EMPTY_BEANS, false)
                .findAndRegisterModules();

        legacyContentResolver = checkNotNull(builder.legacyContentResolver);
        contentStore = checkNotNull(builder.contentStore);
        nullMessageSendingContentStore = builder.persistence.nullMessageSendingContentStore();
        equivalenceGraphStore = checkNotNull(builder.contentEquivalenceGraphStore);
        equivalentContentStore = checkNotNull(builder.equivalentContentStore);
        nullMessageSendingEquivalentContentStore = builder.persistence.nullMessageSendingEquivalentContentStore();
        neo4jContentStore = checkNotNull(builder.neo4jContentStore);

        contentBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .build();

        nullMessageSendingContentBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(nullMessageSendingContentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(nullMessageSendingEquivalentContentStore)
                .build();

        contentAndHierarchyBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .withMigrateHierarchies(builder.legacySegmentMigrator, legacyContentResolver)
                .build();

        nullMessageSendingContentAndHierarchyBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(nullMessageSendingContentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(nullMessageSendingEquivalentContentStore)
                .withMigrateHierarchies(builder.legacySegmentMigrator, legacyContentResolver)
                .build();

        contentAndEquivBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .withMigrateEquivalents(builder.persistence.nullMessageSendingEquivalenceGraphStore())
                .build();

        nullMessageSendingContentAndEquivBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(nullMessageSendingContentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(nullMessageSendingEquivalentContentStore)
                .withMigrateEquivalents(builder.persistence.nullMessageSendingEquivalenceGraphStore())
                .build();

        contentEquivAndHierarchyBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .withMigrateHierarchies(builder.legacySegmentMigrator, legacyContentResolver)
                .withMigrateEquivalents(builder.persistence.nullMessageSendingEquivalenceGraphStore())
                .build();

        forceWriteBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .withForceWrite(true)
                .build();

        nullMessageSendingContentEquivAndHierarchyBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(nullMessageSendingContentStore)
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withEquivalentContentStore(nullMessageSendingEquivalentContentStore)
                .withMigrateHierarchies(builder.legacySegmentMigrator, legacyContentResolver)
                .withMigrateEquivalents(builder.persistence.nullMessageSendingEquivalenceGraphStore())
                .build();

        contentNeo4jMigrator = ContentNeo4jMigrator.create(
                builder.neo4jContentStore, contentStore, builder.contentEquivalenceGraphStore
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    @RequestMapping("/system/id/decode/uppercase/{id}")
    public void decodeUppercaseId(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws IOException {
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().write(uppercase.decode(id).toString());
    }

    @RequestMapping("/system/id/decode/lowercase/{id}")
    public void decodeLowercaseId(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws IOException {
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().write(lowercase.decode(id).toString());
    }

    @RequestMapping("/system/id/encode/uppercase/{id}")
    public void encodeUppercaseId(
            @PathVariable("id") Long id,
            final HttpServletResponse response
    ) throws IOException {
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().write(uppercase.encode(BigInteger.valueOf(id)));
    }

    @RequestMapping("/system/id/encode/lowercase/{id}")
    public void encodeLowercaseId(
            @PathVariable("id") Long id,
            final HttpServletResponse response
    ) throws IOException {
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().write(lowercase.encode(BigInteger.valueOf(id)));
    }

    @RequestMapping("/system/id/toLowercase/{id}")
    public void toLowercaseId(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws IOException {
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().write(lowercase.encode(uppercase.decode(id)));
    }

    @RequestMapping("/system/id/toUppercase/{id}")
    public void toUppercaseId(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws IOException {
        response.setContentType("text/html;charset=UTF-8");
        response.getWriter().write(uppercase.encode(lowercase.decode(id)));
    }

    /* Deactivates a piece of content by setting activelyPublished to false */
    @RequestMapping("/system/debug/content/{id}/deactivate")
    public void deactivateContent(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws IOException {
        try {
            Resolved<Content> resolved = Futures.getChecked(
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
    public void updateEquivalentContentStore(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws IOException {
        try {
            Id contentId = decodeId(id);
            equivalentContentStore.updateContent(contentId);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /* Returns the JSON representation of a legacy content read from Mongo and translated to the v4 model */
    @RequestMapping("/system/debug/content/{id}/legacy")
    public void printLegacyContent(
            @PathVariable("id") String id,
            HttpServletResponse response
    ) throws Exception {
        ListenableFuture<Resolved<Content>> resolving = legacyContentResolver
                .resolveIds(ImmutableList.of(Id.valueOf(lowercase.decode(id))));
        Resolved<Content> resolved = Futures.get(resolving, Exception.class);
        Content content = Iterables.getOnlyElement(resolved.getResources());

        jackson.writeValue(response.getWriter(), content);
        response.flushBuffer();
    }

    /* Returns the JSON representation of a piece of content stored in the Cassandra store */
    @RequestMapping("/system/debug/content/{id}")
    public void printContent(
            @PathVariable("id") String id,
            HttpServletResponse response
    ) throws Exception {
        Id decodedId = decodeId(id);
        ImmutableList<Id> ids = ImmutableList.of(decodedId);
        Resolved<Content> result = Futures.get(
                contentStore.resolveIds(ids), 1, TimeUnit.MINUTES, Exception.class
        );
        Content content = result.getResources().first().orNull();

        jackson.writeValue(response.getWriter(), content);
        response.flushBuffer();
    }

    /* Returns the JSON representation of a piece of content stored in the equivalent content store */
    @RequestMapping("/system/debug/equivalentcontent/{id}")
    public void printEquivalentContent(
            @PathVariable("id") String idString,
            HttpServletResponse response
    ) throws Exception {
        Id id = decodeId(idString);
        ImmutableList<Id> ids = ImmutableList.of(id);
        ResolvedEquivalents<Content> result = Futures.get(
                equivalentContentStore.resolveIds(
                        ids, Publisher.all(), ImmutableSet.of(), null
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
    public void printEquivalentContentSet(
            @PathVariable("id") String idString,
            HttpServletResponse response
    ) throws Exception {
        Id id = decodeId(idString);
        ImmutableList<Id> ids = ImmutableList.of(id);
        ResolvedEquivalents<Content> result = Futures.get(
                equivalentContentStore.resolveIds(
                        ids, Publisher.all(), ImmutableSet.of(), null
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
    public void printContentEquivalence(
            @PathVariable("id") String id,
            final HttpServletResponse response
    ) throws Exception {
        Id decodedId = decodeId(id);
        ImmutableList<Id> ids = ImmutableList.of(decodedId);
        OptionalMap<Id, EquivalenceGraph> equivalenceGraph = Futures.get(
                equivalenceGraphStore.resolveIds(ids),
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

    @RequestMapping("/system/debug/content/{id}/migrate")
    public void forceEquivUpdate(
            @PathVariable("id") String id,
            @RequestParam(name = "equivalents", defaultValue = "false") boolean migrateEquivalents,
            @RequestParam(name = "hierarchy", defaultValue = "true") boolean migrateHierarchy,
            @RequestParam(name = "sendKafkaMessages", defaultValue = "true") boolean sendKafkaMessages,
            final HttpServletResponse response
    ) throws IOException {
        Content content = getContentById(id);
        migrateContent(migrateEquivalents, migrateHierarchy, sendKafkaMessages, response, content);
    }

    @RequestMapping("/system/debug/content/migrate")
    public void forceListEquivUpdate(
            @RequestParam(name = "ids", defaultValue = "") String ids,
            @RequestParam(name = "uris", defaultValue = "") String uris,
            @RequestParam(name = "equivalents", defaultValue = "false") boolean migrateEquivalents,
            @RequestParam(name = "hierarchy", defaultValue = "true") boolean migrateHierarchy,
            @RequestParam(name = "sendKafkaMessages", defaultValue = "true") boolean sendKafkaMessages,
            final HttpServletResponse response
    ) throws IOException {
        if (Strings.isNullOrEmpty(ids) && Strings.isNullOrEmpty(uris)) {
            throw new IllegalArgumentException("Must specify at least one content ID or URI to "
                                               + "migrate (parameters \"ids\" or \"uris\", can be "
                                               + "comma separated lists)");
        }

        Iterable<String> requestedIds = commaSplitter.split(ids);
        for (String id : requestedIds) {
            Content content = getContentById(id);
            migrateContent(migrateEquivalents, migrateHierarchy, sendKafkaMessages, response, content);
        }

        Iterable<String> requestedUris = commaSplitter.split(uris);
        for (String uri : requestedUris) {
            Content content = getContentByUri(uri);
            migrateContent(migrateEquivalents, migrateHierarchy, sendKafkaMessages, response, content);
        }
    }

    @RequestMapping("/system/debug/content/force/migrate")
    public void forceListEquivUpdate(
            @RequestParam(name = "ids", defaultValue = "") String ids,
            @RequestParam(name = "uris", defaultValue = "") String uris,
            final HttpServletResponse response
    ) throws IOException {
        if (Strings.isNullOrEmpty(ids) && Strings.isNullOrEmpty(uris)) {
            throw new IllegalArgumentException("Must specify at least one content ID or URI to "
                                               + "migrate (parameters \"ids\" or \"uris\", can be "
                                               + "comma separated lists)");
        }

        Iterable<String> requestedIds = commaSplitter.split(ids);
        for (String id : requestedIds) {
            Content content = getContentById(id);
            forceMigrateContent(response, content);
        }

        Iterable<String> requestedUris = commaSplitter.split(uris);
        for (String uri : requestedUris) {
            Content content = getContentByUri(uri);
            forceMigrateContent(response, content);
        }
    }

    private void migrateContent(
            boolean migrateEquivalents,
            boolean migrateHierarchy,
            boolean sendKafkaMessages,
            HttpServletResponse response,
            Content content
    ) throws IOException {
        try {

            ContentBootstrapListener listener;
            if(migrateEquivalents && migrateHierarchy){
                listener = sendKafkaMessages
                           ? contentEquivAndHierarchyBootstrapListener
                           : nullMessageSendingContentEquivAndHierarchyBootstrapListener;
            } else if(migrateEquivalents){
                listener = sendKafkaMessages
                           ? contentAndEquivBootstrapListener
                           : nullMessageSendingContentAndEquivBootstrapListener;
            } else if(migrateHierarchy){
                listener = sendKafkaMessages
                           ? contentAndHierarchyBootstrapListener
                           : nullMessageSendingContentAndHierarchyBootstrapListener;
            } else {
                listener = sendKafkaMessages
                           ? contentBootstrapListener
                           : nullMessageSendingContentBootstrapListener;
            }

            ContentBootstrapListener.Result result = content.accept(listener);

            response.setStatus(HttpStatus.OK.value());
            response.getWriter().println(result.toString());
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }

    private void forceMigrateContent(
            HttpServletResponse response,
            Content content
    ) throws IOException {
        try {

            ContentBootstrapListener.Result result = content.accept(forceWriteBootstrapListener);

            response.setStatus(HttpStatus.OK.value());
            response.getWriter().println(result.toString());
            response.flushBuffer();
        } catch (Throwable t) {
            t.printStackTrace(response.getWriter());
        }
    }

    private Content getContentById(String id) {
        Long decodedId = lowercase.decode(id).longValue();

        return Iterables.getOnlyElement(
                Futures.getUnchecked(
                        legacyContentResolver.resolveIds(ImmutableList.of(Id.valueOf(decodedId)))
                ).getResources());
    }

    private Content getContentByUri(String uri) throws IllegalArgumentException {

        sanitizeUri(uri);

        return Iterables.getOnlyElement(
                Futures.getUnchecked(
                        legacyContentResolver.resolveUri(uri)
                ).getResources());
    }

    private void sanitizeUri(String uri) throws IllegalArgumentException {
        if(ILLEGAL_URI_CHARACTERS.matcher(uri).find()){
            throw new IllegalArgumentException("Given uri contains illegal characters, i.e. ' \" \\ ; { } ");
        }
    }

    @RequestMapping(value = "/system/debug/content/{id}/neo4j", method = RequestMethod.POST)
    public void updateContentInNeo4j(
            @PathVariable("id") String id,
            HttpServletResponse response
    ) throws IOException {
        PrintWriter writer = response.getWriter();

        try {
            Id decodedId = decodeId(id);

            ContentNeo4jMigrator.Result result = contentNeo4jMigrator.migrate(decodedId, true);

            writer.println("Migrating " + result.getId());

            if (result.getSuccess()) {
                response.setStatus(HttpStatus.OK.value());
                writer.println("Success");
            } else {
                response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
                writer.println("Failure\n");
                writer.println(result.getMessage());
            }

            switch (result.getGraphMigrationResult()) {
            case FULL:
                writer.println("Migrated full graph");
                break;
            case ADJACENTS_ONLY:
                writer.write("Migrated graph");
                break;
            case NONE:
                writer.write("Did not migrate graph");
            }

        } catch (Exception e) {
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        response.flushBuffer();
    }

    @RequestMapping(value = "/system/debug/content/{id}/neo4j", method = RequestMethod.GET)
    public void getContentGraphFromNeo4j(
            @PathVariable("id") String id,
            HttpServletResponse response
    ) throws IOException {
        try {
            Id decodedId = decodeId(id);

            ImmutableSet<Id> equivalentSet = neo4jContentStore.getEquivalentSet(decodedId);

            jackson.writeValue(
                    response.getWriter(),
                    ImmutableMap.of(
                            "requestedId", decodedId.longValue(),
                            "equivalentSet", equivalentSet.stream()
                                    .map(Id::longValue)
                                    .collect(MoreCollectors.toImmutableSet())
                    )
            );
        } catch (Exception e) {
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
            jackson.writeValue(
                    response.getWriter(),
                    ImmutableMap.of(
                            "error", e.getMessage(),
                            "type", e.getClass().getCanonicalName(),
                            "stackTrace", Throwables.getStackTraceAsString(e)
                    )
            );
        }
        response.flushBuffer();
    }

    private Id decodeId(String id) {
        return Id.valueOf(lowercase.decode(id).longValue());
    }

    public static final class Builder {

        private LegacyContentResolver legacyContentResolver;
        private ContentStore contentStore;
        private EquivalentContentStore equivalentContentStore;
        private Neo4jContentStore neo4jContentStore;
        private LegacySegmentMigrator legacySegmentMigrator;
        private AtlasPersistenceModule persistence;
        private DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
        private EquivalenceGraphStore contentEquivalenceGraphStore;

        private Builder() { }

        public Builder withContentEquivalenceGraphStore(EquivalenceGraphStore val) {
            contentEquivalenceGraphStore = val;
            return this;
        }

        public Builder withEquivalenceMigrator(DirectAndExplicitEquivalenceMigrator val) {
            this.equivalenceMigrator = val;
            return this;
        }

        public Builder withPersistence(AtlasPersistenceModule val) {
            persistence = val;
            return this;
        }

        public Builder withLegacySegmentMigrator(LegacySegmentMigrator val) {
            legacySegmentMigrator = val;
            return this;
        }

        public Builder withLegacyContentResolver(LegacyContentResolver val) {
            legacyContentResolver = val;
            return this;
        }

        public Builder withContentStore(ContentStore val) {
            contentStore = val;
            return this;
        }

        public Builder withEquivalentContentStore(EquivalentContentStore val) {
            equivalentContentStore = val;
            return this;
        }

        public Builder withNeo4jContentStore(Neo4jContentStore val) {
            neo4jContentStore = val;
            return this;
        }

        public ContentDebugController build() {
            return new ContentDebugController(this);
        }
    }
}
