package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Episode;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.IndexException;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.ProgressStore;

import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ContentBootstrapController {

    private static final Logger log = LoggerFactory.getLogger(ContentBootstrapController.class);

    private final ContentResolver read;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ResourceLister<Content> contentLister;
    private final ContentIndex contentIndex;
    private final Integer maxSourceBootstrapThreads;
    private final ProgressStore progressStore;
    private final Timer timer;

    private final ContentBootstrapListener.Builder contentBootstrapListenerBuilder;
    private final ContentBootstrapListener.Builder contentAndEquivalentsBootstrapListenerBuilder;
    private final ContentNeo4jMigrator contentNeo4jMigrator;

    private final ContentResolver legacyResolver;
    private final ContentStore contentStore;
    private final Function<Worker<ResourceUpdatedMessage>, KafkaConsumer> replayConsumerFactory;

    private final ContentWriter contentWriter;
    private final ContentWriter nullMessageSenderContentWriter;
    private final EquivalentContentStore equivalentContentStore;
    private final EquivalentContentStore nullMessageSenderEquivalentContentStore;

    private KafkaConsumer replayBootstrapListener;

    private ContentBootstrapController(Builder builder) {
        read = checkNotNull(builder.read);
        contentLister = checkNotNull(builder.contentLister);
        contentIndex = checkNotNull(builder.contentIndex);
        maxSourceBootstrapThreads = checkNotNull(builder.maxSourceBootstrapThreads);
        progressStore = checkNotNull(builder.progressStore);
        timer = checkNotNull(builder.metrics).timer(getClass().getSimpleName());
        legacyResolver = checkNotNull(builder.legacyResolver);
        contentStore = checkNotNull(builder.contentStore);
        contentWriter = checkNotNull(builder.write);
        nullMessageSenderContentWriter = checkNotNull(builder.nullMessageSenderWrite);
        equivalentContentStore = checkNotNull(builder.equivalentContentStore);
        nullMessageSenderEquivalentContentStore = checkNotNull(builder.nullMessageSenderEquivalentContentStore);
        replayConsumerFactory = checkNotNull(builder.replayConsumerFactory);

        contentBootstrapListenerBuilder = ContentBootstrapListener.builder()
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withSegmentMigratorAndContentResolver(null, legacyResolver)
                .withContentIndex(contentIndex);

        contentAndEquivalentsBootstrapListenerBuilder = ContentBootstrapListener.builder()
                .withEquivalenceMigrator(builder.equivalenceMigrator)
                .withSegmentMigratorAndContentResolver(null, legacyResolver)
                .withContentIndex(contentIndex)
                .withMigrateEquivalents(builder.equivalenceGraphStore);

        contentNeo4jMigrator = ContentNeo4jMigrator.create(
                builder.neo4JContentStore,
                builder.contentStore,
                builder.equivalenceGraphStore
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    @RequestMapping(value = "/system/bootstrap/source", method = RequestMethod.POST)
    public void bootstrapSource(
            @RequestParam("source") String sourceString,
            @RequestParam(name = "equivalents", defaultValue = "false") Boolean migrateEquivalents,
            @RequestParam(name = "sendKafkaMessages", defaultValue = "true") boolean sendKafkaMessages,
            HttpServletResponse resp
    ) throws IOException {
        log.info("Bootstrapping source: {}", sourceString);

        Optional<Publisher> fromKey = Publisher.fromKey(sourceString).toOptional();

        Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.get().toString());

        Publisher source = fromKey.get();
        Runnable listener;
        if (Boolean.TRUE.equals(migrateEquivalents)) {
            listener = bootstrappingRunnable(
                    getContentBootstrapListener(
                            contentAndEquivalentsBootstrapListenerBuilder,
                            sendKafkaMessages
                    ),
                    source,
                    progress
            );
        } else {
            listener = bootstrappingRunnable(
                    getContentBootstrapListener(contentBootstrapListenerBuilder, sendKafkaMessages),
                    source,
                    progress
            );
        }
        executorService.execute(listener);

        resp.setStatus(HttpStatus.ACCEPTED.value());
        resp.getWriter().println(
                "Starting bootstrap of " + source + " "
                        + (migrateEquivalents ? "with" : "without") + " equivalents"
        );
        resp.getWriter().flush();
    }

    @RequestMapping(value = "/system/bootstrap/type", method = RequestMethod.POST)
    public void bootstrapType(
            @RequestParam("type") String type,
            @RequestParam("progress") String progressId,
            @RequestParam(name = "sendKafkaMessages", defaultValue = "true") boolean sendKafkaMessages,
            HttpServletResponse resp
    ) throws IOException {
        Optional<ContentListingProgress> progress = progressStore.progressForTask(progressId);

        Class<? extends Content> filter;
        switch (type) {
        case "episode":
            filter = Episode.class;
            break;
        case "brand":
            filter = Brand.class;
            break;
        case "series":
            filter = Series.class;
            break;
        case "item":
            filter = Item.class;
            break;
        default:
            throw new IllegalArgumentException(String.format("Unknown content type %s", type));
        }

        FluentIterable<? extends Content> contentIterable;
        if (progress.isPresent()) {
            contentIterable = contentLister.list(progress.get()).filter(filter);
        } else {
            contentIterable = contentLister.list().filter(filter);
        }

        Runnable listener = () -> {
            ExecutorService executor = getExecutor();

            for (Content c : contentIterable) {
                executor.submit(() -> c.accept(getContentBootstrapListener(
                        contentBootstrapListenerBuilder,
                        sendKafkaMessages
                )));
                progressStore.storeProgress(
                        progressId,
                        new ContentListingProgress(null, null, c.getCanonicalUri())
                );
            }

            executor.shutdown();

            progressStore.storeProgress(progressId, ContentListingProgress.START);
        };

        executorService.execute(listener);

        resp.setStatus(HttpStatus.ACCEPTED.value());
        resp.getWriter().println(String.format("Starting bootstrap %s of %s", progressId, type));
        resp.getWriter().flush();
    }

    @RequestMapping(value = "/system/bootstrap/all", method = RequestMethod.POST)
    public void bootstrapAllSources(
            @Nullable @RequestParam("exclude") String excludedSourcesString,
            @RequestParam(name = "sendKafkaMessages", defaultValue = "true") boolean sendKafkaMessages,
            HttpServletResponse resp
    ) {
        Set<Publisher> excludedSources = ImmutableSet.of();

        if (excludedSourcesString != null && !excludedSourcesString.trim().isEmpty()) {
            excludedSources = ImmutableSet.copyOf(Publisher.fromCsv(excludedSourcesString));
        }

        Set<Publisher> sourcesToBootstrap = Sets.difference(Publisher.all(), excludedSources);
        for (Publisher source : sourcesToBootstrap) {
            Optional<ContentListingProgress> progress =
                    progressStore.progressForTask(source.toString());
            executorService.execute(
                    bootstrappingRunnable(
                            getContentBootstrapListener(
                                    contentBootstrapListenerBuilder,
                                    sendKafkaMessages
                            ),
                            source,
                            progress
                    )
            );
        }

        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    @RequestMapping(value = "/system/count/nonGenerics", method = RequestMethod.POST)
    public void countContent(
            @RequestParam("source") final String sourceString,
            HttpServletResponse resp
    ) {
        log.info("Bootstrapping source: {}", sourceString);
        Optional<Publisher> fromKey = Publisher.fromKey(sourceString).toOptional();
        Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.get().toString());
        executorService.execute(contentCountingRunnable(fromKey.get(), progress));
        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    @RequestMapping(value = "/system/bootstrap/content", method = RequestMethod.POST)
    public void bootstrapContent(
            @RequestParam("id") final String id,
            @RequestParam(name = "sendKafkaMessages", defaultValue = "true") boolean sendKafkaMessages,
            final HttpServletResponse resp
    ) throws IOException {
        log.info("Bootstrapping: {}", id);
        Identified identified = Iterables.getOnlyElement(
                resolve(ImmutableList.of(Id.valueOf(id))),
                null
        );
        log.info("Bootstrapping: {} {}", id, identified);
        if (!(identified instanceof Content)) {
            resp.sendError(500, "Resolved not content");
            return;
        }
        Content content = (Content) identified;

        ContentBootstrapListener.Result result = content.accept(getContentBootstrapListener(
                contentBootstrapListenerBuilder,
                sendKafkaMessages
        ));

        if (result.getSucceeded()) {
            resp.setStatus(HttpStatus.OK.value());
        } else {
            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }

        resp.getWriter().println(result.toString());
        resp.getWriter().flush();
    }

    @RequestMapping(value = "/system/bootstrap/kafkaReplay", method = RequestMethod.POST)
    public void replayKafkaContentChanges(
            @RequestParam("since") String dateTimeString,
            HttpServletResponse response
    ) {
        if (this.replayBootstrapListener != null) {
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            return;
        }

        DateTime since = DateTime.parse(dateTimeString);

        this.replayBootstrapListener = replayConsumerFactory.apply(
                message -> {
                    DateTime messageTime = message.getTimestamp().toDateTimeUTC();
                    if (messageTime.isBefore(since)) {
                        log.debug(
                                "Message from {} is before threshold {}, skipping",
                                messageTime,
                                since
                        );
                        return;
                    }

                    ResourceRef updated = message.getUpdatedResource();

                    Content legacy;
                    try {
                        log.debug("Resolving content for {} at {}", updated.getId(), messageTime);
                        legacy = legacyResolver.resolveIds(
                                ImmutableList.of(updated.getId())
                        ).get().getResources().first().orNull();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    if (legacy == null) {
                        return;
                    }

                    try {
                        log.info(
                                "Bootstrapping content for {} at {}",
                                updated.getId(),
                                messageTime
                        );

                        contentStore.writeContent(legacy);
                    } catch (WriteException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        replayBootstrapListener.startAsync().awaitRunning();

        response.setStatus(HttpStatus.ACCEPTED.value());
    }

    @RequestMapping(value = "/system/bootstrap/kafkaReplay", method = RequestMethod.DELETE)
    public void killKafkaReplayWorker(
            HttpServletResponse response
    ) {
        if (replayBootstrapListener != null) {
            replayBootstrapListener.stopAsync().awaitTerminated();
            replayBootstrapListener = null;
            response.setStatus(HttpStatus.OK.value());
        } else {
            response.setStatus(HttpStatus.NOT_FOUND.value());
        }
    }

        @RequestMapping(value = "/system/index/source", method = RequestMethod.POST)
    public void indexSource(
            @RequestParam("source") final String sourceString,
            HttpServletResponse resp
    ) {
        ContentVisitorAdapter<Class<Void>> visitor = contentIndexingVisitor();
        log.info("Bootstrapping source: {}", sourceString);
        Optional<Publisher> fromKey = Publisher.fromKey(sourceString).toOptional();
        Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.get().toString());
        executorService.execute(indexingRunnable(visitor, fromKey.get(), progress));
        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    @RequestMapping(value = "/system/neo4j/source", method = RequestMethod.POST)
    public void bootstrapSourceInNeo4j(
            @RequestParam("source") String sourceString,
            @RequestParam(name = "equivalents", defaultValue = "false") Boolean migrateEquivalents,
            HttpServletResponse resp
    ) {
        log.info("Bootstrapping source in Neo4j: {}", sourceString);
        Publisher source = Publisher.fromKey(sourceString).requireValue();

        Optional<ContentListingProgress> progress = progressStore.progressForTask(
                "Neo4jBootstrap-" + source.key()
        );

        executorService.execute(neo4jBootstrapRunnable(source, progress, migrateEquivalents));

        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    private ContentBootstrapListener getContentBootstrapListener(
            ContentBootstrapListener.Builder builder,
            boolean sendKafkaMessages
    ) {
        if (!sendKafkaMessages) {
            return builder.withContentWriter(nullMessageSenderContentWriter)
                    .withEquivalentContentStore(nullMessageSenderEquivalentContentStore)
                    .build();
        }

        return builder.withContentWriter(contentWriter)
                .withEquivalentContentStore(equivalentContentStore)
                .build();
    }

    private FluentIterable<Content> resolve(Iterable<Id> ids) {
        try {
            ListenableFuture<Resolved<Content>> resolved = read.resolveIds(ids);
            return Futures.getChecked(resolved, IOException.class).getResources();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private Runnable bootstrappingRunnable(
            ContentVisitorAdapter<?> visitor,
            Publisher source,
            Optional<ContentListingProgress> progress
    ) {
        FluentIterable<Content> contentIterable = getContentIterable(source, progress);

        return () -> visitContentIterable(visitor, source, contentIterable);
    }

    private void visitContentIterable(ContentVisitorAdapter<?> visitor, Publisher source,
            FluentIterable<Content> contentIterable) {
        AtomicInteger atomicInteger = new AtomicInteger();
        ExecutorService executor = getExecutor();
        for (Content c : contentIterable) {
            executor.submit(
                    () -> {
                        Timer.Context time = timer.time();
                        int count = atomicInteger.incrementAndGet();
                        log.info(
                                "Bootstrapping content type: {}, id: {}, activelyPublished: {}, uri: {}, count: {}",
                                ContentType.fromContent(c).get(),
                                c.getId(),
                                c.isActivelyPublished(),
                                c.getCanonicalUri(),
                                count
                        );
                        c.accept(visitor);
                        storeProgress(source, c, count);
                        time.stop();
                    }
            );
        }
            /* Finished */
        progressStore.storeProgress(source.toString(), ContentListingProgress.START);
    }

    private Runnable contentCountingRunnable(
            Publisher source,
            Optional<ContentListingProgress> progress
    ) {
        FluentIterable<Content> contentIterable = getContentIterable(source, progress);

        return () -> {
            AtomicInteger genericCount = new AtomicInteger();
            AtomicInteger progressCount = new AtomicInteger();

            ExecutorService executor = getExecutor();

            for (Content content : contentIterable) {
                executor.execute(() -> {
                    if (content.isGenericDescription() != null && content.isGenericDescription()) {
                        if (genericCount.incrementAndGet() % 1000 == 0) {
                            log.info(
                                    "Found {} PA items with generic descriptions",
                                    Integer.valueOf(genericCount.get())
                            );
                        }
                    }
                    if (progressCount.incrementAndGet() % 1000 == 0) {
                        log.info("Processed {} PA items", Integer.valueOf(progressCount.get()));
                        progressStore.storeProgress(
                                source.toString(),
                                progressFrom(content, source)
                        );
                    }
                });
            }
            log.info(
                    "Found a total of {} PA items with generic descriptions",
                    Integer.valueOf(genericCount.get())
            );
        };
    }

    private ContentVisitorAdapter<Class<Void>> contentIndexingVisitor() {
        return new ContentVisitorAdapter<Class<Void>>() {

            @Override
            protected Class<Void> visitItem(Item item) {
                try {
                    contentIndex.index(item);
                } catch (IndexException e) {
                    log.error("Failed to index content", e);
                }
                return Void.TYPE;
            }

            @Override
            protected Class<Void> visitContainer(Container container) {
                try {
                    contentIndex.index(container);
                } catch (IndexException e) {
                    log.error("Failed to index content", e);
                }
                return Void.TYPE;
            }
        };
    }

    private Runnable indexingRunnable(
            ContentVisitorAdapter<Class<Void>> visitor,
            Publisher source,
            Optional<ContentListingProgress> progress
    ) {
        FluentIterable<Content> contentIterable = getContentIterable(source, progress);

        return () -> visitContentIterable(visitor, source, contentIterable);
    }

    private Runnable neo4jBootstrapRunnable(
            Publisher source,
            Optional<ContentListingProgress> progress,
            Boolean migrateEquivalents
    ) {
        return () -> {
            AtomicInteger atomicInteger = new AtomicInteger();

            ExecutorService executor = getExecutor();

            for (Content content : getContentIterable(source, progress)) {
                executor.submit(() -> migrateToNeo4j(
                        content, source, migrateEquivalents, atomicInteger
                ));
            }
            /* Finished */
            progressStore.storeProgress(source.toString(), ContentListingProgress.START);
        };
    }

    private void migrateToNeo4j(
            Content content,
            Publisher source,
            Boolean migrateEquivalents,
            AtomicInteger counter
    ) {
        Timer.Context time = timer.time();
        int count = counter.incrementAndGet();

        log.info(
                "Bootstrapping in Neo4j content type: {}, id: {}, activelyPublished: {}, "
                        + "uri: {}, count: {}",
                ContentType.fromContent(content).get(),
                content.getId(),
                content.isActivelyPublished(),
                content.getCanonicalUri(),
                count
        );

        contentNeo4jMigrator.migrate(content, migrateEquivalents);

        storeProgress(source, content, count);
        time.stop();
    }

    private ExecutorService getExecutor() {
        return new ThreadPoolExecutor(
                        maxSourceBootstrapThreads,
                        maxSourceBootstrapThreads,
                        500,
                        TimeUnit.MILLISECONDS,
                        Queues.newLinkedBlockingQueue(maxSourceBootstrapThreads),
                        new ThreadPoolExecutor.CallerRunsPolicy()
                );
    }

    @SuppressWarnings("Guava")
    private FluentIterable<Content> getContentIterable(
            Publisher source,
            Optional<ContentListingProgress> progress
    ) {
        if (progress.isPresent()) {
            log.info(
                    "Starting bootstrap of {} from Content {}",
                    source.toString(),
                    progress.get().getUri()
            );
            return contentLister.list(ImmutableList.of(source), progress.get());
        } else {
            log.info(
                    "Starting bootstrap of {} the start, no existing progress found",
                    source.toString()
            );
            return contentLister.list(ImmutableList.of(source));
        }
    }

    private ContentListingProgress progressFrom(Content content, Publisher pub) {
        return new ContentListingProgress(null, pub, content.getCanonicalUri());
    }

    private void storeProgress(Publisher source, Content content, int count) {
        if (count % 10000 == 0) {
            progressStore.storeProgress(
                    source.toString(),
                    progressFrom(content, source)
            );
        }
    }

    public static final class Builder {

        private ContentResolver read;
        private ContentWriter write;
        private ContentWriter nullMessageSenderWrite;
        private DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
        private EquivalentContentStore equivalentContentStore;
        private EquivalentContentStore nullMessageSenderEquivalentContentStore;
        private EquivalenceGraphStore equivalenceGraphStore;
        private Neo4jContentStore neo4JContentStore;
        private ContentStore contentStore;
        private MetricRegistry metrics;
        private ResourceLister<Content> contentLister;
        private ContentIndex contentIndex;
        private Integer maxSourceBootstrapThreads;
        private ProgressStore progressStore;

        private ContentResolver legacyResolver;
        private Function<Worker<ResourceUpdatedMessage>, KafkaConsumer> replayConsumerFactory;

        private Builder() { }

        public Builder withLegacyResolver(ContentResolver val) {
            legacyResolver = val;
            return this;
        }

        public Builder withReplayConsumerFactory(
                Function<Worker<ResourceUpdatedMessage>, KafkaConsumer> val
        ) {
            replayConsumerFactory = val;
            return this;
        }

        public Builder withContentStore(ContentStore val) {
            contentStore = val;
            return this;
        }

        public Builder withNeo4JContentStore(Neo4jContentStore val) {
            neo4JContentStore = val;
            return this;
        }

        public Builder withEquivalenceGraphStore(EquivalenceGraphStore val) {
            equivalenceGraphStore = val;
            return this;
        }

        public Builder withEquivalentContentStore(EquivalentContentStore val) {
            equivalentContentStore = val;
            return this;
        }

        public Builder withNullMessageSenderEquivalentContentStore(EquivalentContentStore val) {
            nullMessageSenderEquivalentContentStore = val;
            return this;
        }

        public Builder withEquivalenceMigrator(DirectAndExplicitEquivalenceMigrator val) {
            equivalenceMigrator = val;
            return this;
        }

        public Builder withWrite(ContentWriter val) {
            write = val;
            return this;
        }

        public Builder withNullMessageSenderWrite(ContentWriter val) {
            nullMessageSenderWrite = val;
            return this;
        }

        public Builder withMetrics(MetricRegistry val) {
            metrics = val;
            return this;
        }

        public Builder withRead(ContentResolver val) {
            read = val;
            return this;
        }

        public Builder withContentLister(ResourceLister<Content> val) {
            contentLister = val;
            return this;
        }

        public Builder withContentIndex(ContentIndex val) {
            contentIndex = val;
            return this;
        }

        public Builder withMaxSourceBootstrapThreads(Integer val) {
            maxSourceBootstrapThreads = val;
            return this;
        }

        public Builder withProgressStore(ProgressStore val) {
            progressStore = val;
            return this;
        }

        public ContentBootstrapController build() {
            return new ContentBootstrapController(this);
        }
    }
}
