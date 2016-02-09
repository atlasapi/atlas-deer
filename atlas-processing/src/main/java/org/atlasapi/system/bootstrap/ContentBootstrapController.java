package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.IndexException;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.ProgressStore;

import com.metabroadcast.common.base.Maybe;

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

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentResolver read;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ResourceLister<Content> contentLister;
    private final ContentIndex contentIndex;
    private final Integer maxSourceBootstrapThreads;
    private final ProgressStore progressStore;
    private final Timer timer;

    private final ContentBootstrapListener contentBootstrapListener;
    private final ContentBootstrapListener contentAndEquivalentsBootstrapListener;

    public ContentBootstrapController(
            ContentResolver read,
            ResourceLister<Content> contentLister,
            ContentWriter write,
            ContentIndex contentIndex,
            AtlasPersistenceModule persistence,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator,
            Integer maxSourceBootstrapThreads,
            ProgressStore progressStore,
            MetricRegistry metrics) {
        this.maxSourceBootstrapThreads = maxSourceBootstrapThreads;
        this.read = checkNotNull(read);
        this.contentLister = checkNotNull(contentLister);
        this.contentIndex = checkNotNull(contentIndex);
        this.progressStore = checkNotNull(progressStore);
        this.timer = metrics.timer(getClass().getSimpleName());

        this.contentBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(write)
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withContentIndex(contentIndex)
                .build();

        this.contentAndEquivalentsBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(write)
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(persistence.nullMessageSendingEquivalentContentStore())
                .withContentIndex(contentIndex)
                .withMigrateEquivalents(persistence.nullMessageSendingEquivalenceGraphStore())
                .build();
    }

    @RequestMapping(value = "/system/bootstrap/source", method = RequestMethod.POST)
    public void bootstrapSource(@RequestParam("source") String sourceString,
            @RequestParam(name = "equivalents", defaultValue = "false") Boolean migrateEquivalents,
            HttpServletResponse resp) throws IOException {
        log.info("Bootstrapping source: {}", sourceString);
        Maybe<Publisher> fromKey = Publisher.fromKey(sourceString);
        java.util.Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.toString());

        Publisher source = fromKey.requireValue();
        Runnable listener;
        if (Boolean.TRUE.equals(migrateEquivalents)) {
            listener = bootstrappingRunnable(contentAndEquivalentsBootstrapListener,
                    source, progress
            );
        } else {
            listener = bootstrappingRunnable(contentBootstrapListener,
                    source, progress
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

    @RequestMapping(value = "/system/bootstrap/all", method = RequestMethod.POST)
    public void bootstrapAllSources(@RequestParam("exclude") String excludedSourcesString,
            HttpServletResponse resp) {
        Set<Publisher> excludedSources = ImmutableSet.of();

        if (excludedSourcesString != null && !excludedSourcesString.trim().isEmpty()) {
            excludedSources = ImmutableSet.copyOf(Publisher.fromCsv(excludedSourcesString));
        }

        Set<Publisher> sourcesToBootstrap = Sets.difference(Publisher.all(), excludedSources);
        for (Publisher source : sourcesToBootstrap) {
            java.util.Optional<ContentListingProgress> progress =
                    progressStore.progressForTask(source.toString());
            executorService.execute(
                    bootstrappingRunnable(contentBootstrapListener, source, progress)
            );
        }

        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    @RequestMapping(value = "/system/count/nonGenerics", method = RequestMethod.POST)
    public void countContent(@RequestParam("source") final String sourceString,
            HttpServletResponse resp) {
        log.info("Bootstrapping source: {}", sourceString);
        Maybe<Publisher> fromKey = Publisher.fromKey(sourceString);
        java.util.Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.toString());
        executorService.execute(contentCountingRunnable(fromKey.requireValue(), progress));
        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    @RequestMapping(value = "/system/bootstrap/content", method = RequestMethod.POST)
    public void bootstrapContent(@RequestParam("id") final String id,
            final HttpServletResponse resp) throws IOException {
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

        ContentBootstrapListener.Result result = content.accept(contentBootstrapListener);

        if (result.isSucceeded()) {
            resp.setStatus(HttpStatus.OK.value());
        } else {
            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }

        resp.getWriter().println(result.getMessage());
        resp.getWriter().flush();
    }

    @RequestMapping(value = "/system/index/source", method = RequestMethod.POST)
    public void indexSource(@RequestParam("source") final String sourceString,
            HttpServletResponse resp) {
        ContentVisitorAdapter<Class<Void>> visitor = contentIndexingVisitor();
        log.info("Bootstrapping source: {}", sourceString);
        Maybe<Publisher> fromKey = Publisher.fromKey(sourceString);
        java.util.Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.toString());
        executorService.execute(indexingRunnable(visitor, fromKey.requireValue(), progress));
        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    private FluentIterable<Content> resolve(Iterable<Id> ids) {
        try {
            ListenableFuture<Resolved<Content>> resolved = read.resolveIds(ids);
            return Futures.get(resolved, IOException.class).getResources();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private Runnable bootstrappingRunnable(ContentVisitorAdapter<?> visitor, Publisher source,
            java.util.Optional<ContentListingProgress> progress) {
        FluentIterable<Content> contentIterator;
        if (progress.isPresent()) {
            log.info(
                    "Starting bootstrap of {} from Content {}",
                    source.toString(),
                    progress.get().getUri()
            );
            contentIterator = contentLister.list(ImmutableList.of(source), progress.get());
        } else {
            log.info(
                    "Starting bootstrap of {} the start, no existing progress found",
                    source.toString()
            );
            contentIterator = contentLister.list(ImmutableList.of(source));
        }
        return () ->
        {
            AtomicInteger atomicInteger = new AtomicInteger();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    maxSourceBootstrapThreads,
                    maxSourceBootstrapThreads,
                    500,
                    TimeUnit.MILLISECONDS,
                    Queues.newLinkedBlockingQueue(maxSourceBootstrapThreads),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            for (Content c : contentIterator) {
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
                            if (count % 10000 == 0) {
                                progressStore.storeProgress(
                                        source.toString(),
                                        progressFrom(c, source)
                                );
                            }
                            time.stop();
                        }
                );
            }
            /* Finished */
            progressStore.storeProgress(source.toString(), ContentListingProgress.START);
        };
    }

    private Runnable contentCountingRunnable(Publisher source,
            java.util.Optional<ContentListingProgress> progress) {
        FluentIterable<Content> contentIterator;
        if (progress.isPresent()) {
            log.info(
                    "Starting bootstrap of {} from Content {}",
                    source.toString(),
                    progress.get().getUri()
            );
            contentIterator = contentLister.list(ImmutableList.of(source), progress.get());
        } else {
            log.info(
                    "Starting bootstrap of {} the start, no existing progress found",
                    source.toString()
            );
            contentIterator = contentLister.list(ImmutableList.of(source));
        }
        return () -> {
            AtomicInteger genericCount = new AtomicInteger();
            AtomicInteger progressCount = new AtomicInteger();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    maxSourceBootstrapThreads,
                    maxSourceBootstrapThreads,
                    500,
                    TimeUnit.MILLISECONDS,
                    Queues.newLinkedBlockingQueue(maxSourceBootstrapThreads),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            for (Content content : contentIterator) {
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

    public ContentVisitorAdapter<Class<Void>> contentIndexingVisitor() {
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

    private Runnable indexingRunnable(ContentVisitorAdapter<Class<Void>> visitor, Publisher source,
            java.util.Optional<ContentListingProgress> progress) {
        FluentIterable<Content> contentIterator;
        if (progress.isPresent()) {
            log.info(
                    "Starting bootstrap of {} from Content {}",
                    source.toString(),
                    progress.get().getUri()
            );
            contentIterator = contentLister.list(ImmutableList.of(source), progress.get());
        } else {
            log.info(
                    "Starting bootstrap of {} the start, no existing progress found",
                    source.toString()
            );
            contentIterator = contentLister.list(ImmutableList.of(source));
        }
        return () ->
        {
            AtomicInteger atomicInteger = new AtomicInteger();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    maxSourceBootstrapThreads,
                    maxSourceBootstrapThreads,
                    500,
                    TimeUnit.MILLISECONDS,
                    Queues.newLinkedBlockingQueue(maxSourceBootstrapThreads),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            for (Content c : contentIterator) {
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
                            if (count % 10000 == 0) {
                                progressStore.storeProgress(
                                        source.toString(),
                                        progressFrom(c, source)
                                );
                            }
                            time.stop();
                        }
                );
            }
            /* Finished */
            progressStore.storeProgress(source.toString(), ContentListingProgress.START);
        };
    }

    public ContentListingProgress progressFrom(Content content, Publisher pub) {
        return new ContentListingProgress(null, pub, content.getCanonicalUri());
    }
}
