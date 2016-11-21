package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.BitSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.Described;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.system.legacy.ProgressStore;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
@RequestMapping("/system/bootstrap/cql-content")
public class CqlContentBootstrapController {

    private static final Logger log = LoggerFactory.getLogger(CqlContentBootstrapController.class);

    private static final String TASK_NAME = "cql-mongo-id-bootstrap";
    private static final long PROGRESS_SAVE_FREQUENCY = 100L;
    private static final int APPROXIMATE_MAX_ID = 50_000_000;

    private final ListeningExecutorService listeningExecutorService;
    private final ProgressStore progressStore;
    private final ResourceLister<Content> contentLister;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;

    private final AtomicBoolean runConsumer = new AtomicBoolean(false);
    private final Meter bootstrapMeter;
    private final Meter cqlWriterErrorMeter;
    private final Counter cqlWriterErrorCounter;
    private final Counter bootstrapCounter;

    public static CqlContentBootstrapController create(
            ListeningExecutorService listeningExecutorService,
            ProgressStore progressStore,
            ContentResolver legacyContentResolver,
            ContentWriter cqlContentStore,
            ResourceLister<Content> contentLister,
            MetricRegistry metrics
    ) {
        return new CqlContentBootstrapController(
                listeningExecutorService,
                progressStore,
                legacyContentResolver,
                cqlContentStore,
                contentLister,
                metrics
        );
    }

    private CqlContentBootstrapController(
            ListeningExecutorService listeningExecutorService,
            ProgressStore progressStore,
            ContentResolver legacyContentResolver,
            ContentWriter cqlContentStore,
            ResourceLister<Content> contentLister,
            MetricRegistry metrics
    ) {
        this.contentLister = checkNotNull(contentLister);
        this.listeningExecutorService = checkNotNull(listeningExecutorService);
        this.progressStore = checkNotNull(progressStore);
        this.contentResolver = checkNotNull(legacyContentResolver);
        this.contentWriter = checkNotNull(cqlContentStore);

        this.bootstrapMeter = checkNotNull(metrics).meter(String.format(
                "%s.bootstrap-rate",
                getClass().getSimpleName()
        ));
        this.bootstrapCounter = checkNotNull(metrics).counter(String.format(
                "%s.bootstrap-count",
                getClass().getSimpleName()
        ));
        this.cqlWriterErrorMeter = checkNotNull(metrics).meter(String.format(
                "%s.cql-writer-error-rate",
                getClass().getSimpleName()
        ));
        this.cqlWriterErrorCounter = checkNotNull(metrics).counter(String.format(
                "%s.cql-writer-error-count",
                getClass().getSimpleName()
        ));
    }

    @RequestMapping(method = RequestMethod.POST)
    public void bootstrap(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        startBootstrapThread();

        response.setStatus(200);
        try {
            response.getWriter().write("Started bootstrap");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public void stopBootstrap(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ExecutionException, InterruptedException, WriteException {
        runConsumer.set(false);

        response.setStatus(200);
        try {
            response.getWriter().write("Stopped bootstrap");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping(method = RequestMethod.GET)
    public void getStatus(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ExecutionException, InterruptedException, WriteException {
        response.setStatus(200);
        try {
            response.getWriter()
                    .write(String.format(
                            "Bootstrap running: %s%n",
                            runConsumer.get()
                    ));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void startBootstrapThread() {
        listeningExecutorService.submit(() -> {
            Optional<ContentListingProgress> storedProgress =
                    progressStore.progressForTask(TASK_NAME);
            ContentListingProgress progress = storedProgress.orElse(ContentListingProgress.START);

            // Let's avoid sending brands and whatnot many times. Since each episode will reference
            // its brand and series, this could be costly if we don't take care.
            //
            // For 50kk IDs, this should take
            // about 6.25M of memory, which we can easily afford.
            BitSet sentAlready = new BitSet(APPROXIMATE_MAX_ID);

            long numProcessed = 0L;

            try {
                for (Content content : contentLister.list(progress)) {
                    if (content == null) {
                        continue;
                    }

                    bootstrap(content, sentAlready);

                    numProcessed++;

                    storeProgress(numProcessed, content);

                    bootstrapMeter.mark();
                    bootstrapCounter.inc();

                    if (!runConsumer.get()) {
                        log.info("Stopping CQL bootstrap Mongo ID producer");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Error while listing Mongo IDs, restarting", e);
                startBootstrapThread();
            }
        });
    }

    private void bootstrap(Content content, BitSet sentAlready) {
        if (content instanceof Episode) {
            Episode episode = (Episode) content;

            ContainerRef brand = episode.getContainerRef();
            SeriesRef series = episode.getSeriesRef();

            if (brand != null && ! sentAlready.get(toInt(brand))) {
                bootstrapFromMongo(brand);
                sentAlready.set(toInt(brand));
            }

            if (series != null && ! sentAlready.get(toInt(series))) {
                bootstrapFromMongo(series);
                sentAlready.set(toInt(series));
            }
        } else if (content instanceof Series) {
            Series series = (Series) content;

            BrandRef brand = series.getBrandRef();

            if (brand != null && ! sentAlready.get(toInt(brand))) {
                bootstrapFromMongo(brand);
                sentAlready.set(toInt(brand));
            }
        }

        ContentRef contentRef = content.toRef();
        bootstrapFromMongo(contentRef);
        sentAlready.set(toInt(contentRef));
    }

    private int toInt(ContentRef brand) {
        return Math.toIntExact(brand.getId().longValue());
    }

    private void bootstrapFromMongo(@Nullable ContentRef contentRef) {
        if (contentRef == null) {
            return;
        }

        try {
            com.google.common.base.Optional<Content> contentInMongo = contentResolver
                    .resolveIds(ImmutableList.of(contentRef.getId()))
                    .get(30L, TimeUnit.SECONDS)
                    .getResources()
                    .first();

            if (! contentInMongo.isPresent()) {
                throw new IllegalArgumentException(String.format(
                        "Content %d not found in Mongo",
                        contentRef.getId().longValue()
                ));
            }

            contentWriter.writeContent(contentInMongo.get());

        } catch (Exception e) {
            cqlWriterErrorMeter.mark();
            cqlWriterErrorCounter.inc();
            throw Throwables.propagate(e);
        }
    }

    private void storeProgress(long numProcessed, Content lastProcessed) {
        if (numProcessed % PROGRESS_SAVE_FREQUENCY == 0L) {
            progressStore.storeProgress(
                    TASK_NAME,
                    new ContentListingProgress(
                            categoryFor(lastProcessed),
                            lastProcessed.getSource(),
                            lastProcessed.getCanonicalUri()
                    )
            );
        }
    }

    // this is a copy-pasta crutch because there are only methods for the old content model there
    // and we have no inverse transformer that does new -> old
    private static ContentCategory categoryFor(Described c) {
        if (c instanceof Item) {
            Item item = (Item) c;
            if (item.getContainerRef() != null) {
                return ContentCategory.CHILD_ITEM;
            } else {
                return ContentCategory.TOP_LEVEL_ITEM;
            }
        } else if (c instanceof Container) {
            if (c instanceof Series && ((Series) c).getBrandRef() != null) {
                return ContentCategory.PROGRAMME_GROUP;
            }
            return ContentCategory.CONTAINER;
        } else if (c instanceof ContentGroup) {
            return ContentCategory.CONTENT_GROUP;
        } else {
            return null;
        }
    }
}
