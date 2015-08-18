package org.atlasapi.system.bootstrap;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.hash.BloomFilter;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.ProgressStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ContentBootstrapController {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentResolver read;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ContentWriter write;
    private final ResourceLister<Content> contentLister;
    private final ContentIndex contentIndex;
    private final AtlasPersistenceModule persistence;
    private final DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
    private final Integer maxSourceBootstrapThreads;
    private final ProgressStore progressStore;
    private final Timer timer;

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
        this.persistence = checkNotNull(persistence);
        this.equivalenceMigrator = checkNotNull(equivalenceMigrator);
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
        this.contentLister = checkNotNull(contentLister);
        this.contentIndex = checkNotNull(contentIndex);
        this.progressStore = checkNotNull(progressStore);
        this.timer = metrics.timer(getClass().getSimpleName());
    }

    @RequestMapping(value = "/system/bootstrap/source", method = RequestMethod.POST)
    public void bootstrapSource(@RequestParam("source") final String sourceString, HttpServletResponse resp) {
        ContentVisitorAdapter<String> visitor = visitor();
        log.info("Bootstrapping source: {}", sourceString);
        Maybe<Publisher> fromKey = Publisher.fromKey(sourceString);
        java.util.Optional<ContentListingProgress> progress = progressStore.progressForTask(fromKey.toString());
        executorService.execute(bootstrappingRunnable(visitor, fromKey.requireValue(), progress));
        resp.setStatus(HttpStatus.ACCEPTED.value());
    }

    private Runnable bootstrappingRunnable(ContentVisitorAdapter<String> visitor, Publisher source, java.util.Optional<ContentListingProgress> progress) {
        FluentIterable<Content> contentIterator;
        if (progress.isPresent()) {
            log.info("Starting bootstrap of {} from Content {}", source.toString(), progress.get().getUri());
            contentIterator = contentLister.list(ImmutableList.of(source), progress.get());
        } else {
            log.info("Starting bootstrap of {} the start, no existing progress found", source.toString());
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
                    Queues.newLinkedBlockingQueue(maxSourceBootstrapThreads * 3),
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
                                progressStore.storeProgress(source.toString(), progressFrom(c, source));
                            }
                            time.stop();
                        }
                );
            }
            /* Finished */
            progressStore.storeProgress(source.toString(), ContentListingProgress.START);
        };
    }

    @RequestMapping(value = "/system/bootstrap/content", method = RequestMethod.POST)
    public void bootstrapContent(@RequestParam("id") final String id, final HttpServletResponse resp) throws IOException {
        log.info("Bootstrapping: {}", id);
        Identified identified = Iterables.getOnlyElement(resolve(ImmutableList.of(Id.valueOf(id))), null);
        log.info("Bootstrapping: {} {}", id, identified);
        if (!(identified instanceof Content)) {
            resp.sendError(500, "Resolved not content");
            return;
        }
        Content content = (Content) identified;

        resp.setStatus(HttpStatus.OK.value());

        String result = content.accept(visitor());
        resp.getWriter().println(result);
        resp.getWriter().flush();
    }

    private FluentIterable<Content> resolve(Iterable<Id> ids) {
        try {
            ListenableFuture<Resolved<Content>> resolved = read.resolveIds(ids);
            return Futures.get(resolved, IOException.class).getResources();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private ContentVisitorAdapter<String> visitor() {
        return new ContentVisitorAdapter<String>() {

            @Override
            public String visit(Brand brand) {
                WriteResult<?, Content> brandWrite = write(brand);
                return String.format("%s", brandWrite);
            }

            @Override
            public String visit(Series series) {
                WriteResult<?, Content> seriesWrite = write(series);
                return String.format("%s", seriesWrite);
            }

            @Override
            protected String visitItem(Item item) {
                return write(item).toString();
            }

            private WriteResult<? extends Content, Content> write(Content content) {
                try {
                    content.setReadHash(null);
                    Instant start = Instant.now();
                    WriteResult<Content, Content> writeResult = write.writeContent(content);
                    Instant cassandraWriteEnd = Instant.now();
                    contentIndex.index(content);
                    Instant indexingEnd = Instant.now();
                    Optional<EquivalenceGraphUpdate> graphUpdate =
                            equivalenceMigrator.migrateEquivalence(content);
                    Instant equivalenceUpdateEnd = Instant.now();
                    persistence.getEquivalentContentStore().updateContent(content.toRef());
                    Instant equivalenceContentUpdateEnd = Instant.now();
                    if (graphUpdate.isPresent()) {
                        persistence.getEquivalentContentStore().updateEquivalences(graphUpdate.get());
                    }
                    Instant end = Instant.now();

                    log.info(
                            "Update for {} write: {}ms, index: {}ms, equivalnce migration: {}ms, equivalent content update {}ms, total: {}ms",
                            content.getId(),
                            Duration.between(start, cassandraWriteEnd).toMillis(),
                            Duration.between(cassandraWriteEnd, indexingEnd).toMillis(),
                            Duration.between(indexingEnd, equivalenceUpdateEnd).toMillis(),
                            Duration.between(equivalenceUpdateEnd, equivalenceContentUpdateEnd).toMillis(),
                            Duration.between(start, end).toMillis()
                    );
                    return writeResult;
                } catch (Exception e) {
                    log.error(String.format("Bootstrapping: %s %s", content.getId(), content), e);
                    return null;
                }
            }

        };
    }

    public ContentListingProgress progressFrom(Content content, Publisher pub) {
        return new ContentListingProgress(null, pub, content.getCanonicalUri());
    }
}
