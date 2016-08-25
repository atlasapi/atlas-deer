package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.BitSet;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.system.legacy.ProgressStore;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.elasticsearch.common.lang3.ObjectUtils;
import org.joda.time.DateTime;
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
    private static final String KAFKA_TOPIC_NAME = "CqlMondoIdBootstrapSender";
    private static final long PROGRESS_SAVE_FREQUENCY = 100L;
    private static final int APPROXIMATE_MAX_ID = 50_000_000;
    private static final String KAFKA_CONSUMER_GROUP = "CqlMongoBootstrapConsumer";

    private final ListeningExecutorService listeningExecutorService;
    private final ProgressStore progressStore;
    private final ResourceLister<Content> contentLister;
    private final MessageSenderFactory messageSenderFactory;
    private final MessageConsumerFactory<KafkaConsumer> messageConsumerFactory;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final DatabasedMongo mongo;

    private final AtomicBoolean runConsumer = new AtomicBoolean(true);
    private MessageSender<ResourceUpdatedMessage> sender = null;
    private KafkaConsumer cqlConsumer = null;

    public static CqlContentBootstrapController create(
            ListeningExecutorService listeningExecutorService,
            DatabasedMongo databasedMongo,
            ProgressStore progressStore,
            ContentResolver legacyContentResolver,
            ContentWriter cqlContentStore,
            ResourceLister<Content> contentLister,
            MessageSenderFactory messageSenderFactory,
            MessageConsumerFactory<KafkaConsumer> messageConsumerFactory
    ){
        return new CqlContentBootstrapController(
                listeningExecutorService,
                databasedMongo,
                progressStore,
                legacyContentResolver,
                cqlContentStore,
                contentLister,
                messageSenderFactory,
                messageConsumerFactory
        );
    }

    private CqlContentBootstrapController(
            ListeningExecutorService listeningExecutorService,
            DatabasedMongo databasedMongo,
            ProgressStore progressStore,
            ContentResolver legacyContentResolver,
            ContentWriter cqlContentStore,
            ResourceLister<Content> contentLister,
            MessageSenderFactory messageSenderFactory,
            MessageConsumerFactory<KafkaConsumer> messageConsumerFactory
    ) {
        this.contentLister = checkNotNull(contentLister);
        this.mongo = checkNotNull(databasedMongo);
        this.listeningExecutorService = checkNotNull(listeningExecutorService);
        this.progressStore = checkNotNull(progressStore);
        this.contentResolver = checkNotNull(legacyContentResolver);
        this.contentWriter = checkNotNull(cqlContentStore);
        this.messageSenderFactory = checkNotNull(messageSenderFactory);
        this.messageConsumerFactory = checkNotNull(messageConsumerFactory);
    }

    @RequestMapping(value = "/write-mongo-ids", method = RequestMethod.POST)
    public void writeIdsToCqlStore(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        if (this.cqlConsumer == null) {
            this.cqlConsumer = messageConsumerFactory.createConsumer(
                    new CqlWritingConsumer(),
                    JacksonMessageSerializer.forType(ResourceUpdatedMessage.class),
                    KAFKA_TOPIC_NAME,
                    KAFKA_CONSUMER_GROUP
            ).withFailedMessagePersistence(mongo)
                    .build();
        }

        cqlConsumer.startAsync().awaitRunning();

        response.setStatus(200);
        try {
            response.getWriter().write("Started Mongo to CQL consumer");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping(value = "/write-mongo-ids", method = RequestMethod.GET)
    public void getCqlStoreWriterRunning(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        boolean isRunning = false;
        if (cqlConsumer != null) {
            isRunning = cqlConsumer.isRunning();
        }

        response.setStatus(200);
        try {
            response.getWriter().write(String.format("CQL writer running: %s%n", isRunning));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping(value = "/write-mongo-ids", method = RequestMethod.DELETE)
    public void stopWritingIdsToCql(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        if (cqlConsumer != null) {
            cqlConsumer.stopAsync().awaitTerminated();
        }

        response.setStatus(200);
        try {
            response.getWriter().write("Stopped Mongo to CQL consumer");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping(value = "/list-mongo-ids", method = RequestMethod.POST)
    public void writeIdsToQueue(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ExecutionException, InterruptedException, WriteException {
        Optional<ContentListingProgress> storedProgress = progressStore.progressForTask(TASK_NAME);
        ContentListingProgress progress = storedProgress.orElse(ContentListingProgress.START);

        initSender();

        listeningExecutorService.submit(() -> {
            // Let's avoid sending brands and whatnot many times. Since each episode will reference
            // its brand and series, this could be costly if we don't take care.
            //
            // For 50kk IDs, this should take
            // about 6.25M of memory, which we can easily afford.
            BitSet sentAlready = new BitSet(APPROXIMATE_MAX_ID);

            long numProcessed = 0L;

            for (Content content : contentLister.list(progress)) {
                bootstrap(content, sentAlready);

                numProcessed++;

                storeProgress(numProcessed, content);

                if (!runConsumer.get()) {
                    log.info("Stopping CQL bootstrap Mongo ID producer");
                    break;
                }
            }
        });

        response.setStatus(200);
        try {
            response.getWriter().write("Started message sender");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
    @RequestMapping(value = "/list-mongo-ids", method = RequestMethod.DELETE)
    public void stopMongoIdWriter(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ExecutionException, InterruptedException, WriteException {
        runConsumer.set(false);

        response.setStatus(200);
        try {
            response.getWriter().write("Stopped Mongo ID message sender");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @RequestMapping(value = "/list-mongo-ids", method = RequestMethod.GET)
    public void isMongoIdSenderRunning(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ExecutionException, InterruptedException, WriteException {
        response.setStatus(200);
        try {
            response.getWriter()
                    .write(String.format(
                            "Mongo ID message sender running: %s%n",
                            runConsumer.get()
                    ));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void initSender() {
        this.sender = checkNotNull(messageSenderFactory).makeMessageSender(
                KAFKA_TOPIC_NAME,
                JacksonMessageSerializer.forType(ResourceUpdatedMessage.class)
        );
    }

    private void bootstrap(Content content, BitSet sentAlready) {
        if (content instanceof Episode) {
            Episode episode = (Episode) content;

            ContainerRef brand = episode.getContainerRef();
            SeriesRef series = episode.getSeriesRef();
            EpisodeRef episodeRef = episode.toRef();

            ContentRef partition = ObjectUtils.firstNonNull(brand, series, episodeRef);

            if (brand != null && ! sentAlready.get(toInt(brand))) {
                sendMessage(brand, partition);
                sentAlready.set(toInt(brand));
            }

            if (series != null && ! sentAlready.get(toInt(series))) {
                sendMessage(series, partition);
                sentAlready.set(toInt(series));
            }

            sendMessage(episodeRef, partition);
        } else if (content instanceof Series) {
            Series series = (Series) content;

            BrandRef brand = series.getBrandRef();
            SeriesRef seriesRef = series.toRef();

            ContentRef partition = ObjectUtils.firstNonNull(brand, seriesRef);

            if (brand != null && ! sentAlready.get(toInt(brand))) {
                sendMessage(brand, partition);
                sentAlready.set(toInt(brand));
            }

            sendMessage(seriesRef, partition);
            sentAlready.set(toInt(seriesRef));
        } else {
            ContentRef contentRef = content.toRef();

            sendMessage(contentRef, contentRef);
            sentAlready.set(toInt(contentRef));
        }
    }

    private int toInt(ContentRef brand) {
        return Math.toIntExact(brand.getId().longValue());
    }

    private void sendMessage(@Nullable ContentRef contentRef, ContentRef partition) {
        if (contentRef == null) {
            return;
        }

        try {
            sender.sendMessage(
                    new ResourceUpdatedMessage(
                            UUID.randomUUID().toString(),
                            Timestamp.of(DateTime.now()),
                            contentRef
                    ),
                    Longs.toByteArray(partition.getId().longValue())
            );
        } catch (MessagingException e) {
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

    private class CqlWritingConsumer implements Worker<ResourceUpdatedMessage> {

        @Override
        public void process(ResourceUpdatedMessage message) throws RecoverableException {
            ResourceRef updated = message.getUpdatedResource();
            try {
                com.google.common.base.Optional<Content> contentInMongo = contentResolver
                        .resolveIds(ImmutableList.of(updated.getId()))
                        .get(30L, TimeUnit.SECONDS)
                        .getResources()
                        .first();

                if (! contentInMongo.isPresent()) {
                    throw new IllegalArgumentException(String.format(
                            "Content %d not found in Mongo",
                            updated.getId().longValue()
                    ));
                }

                contentWriter.writeContent(contentInMongo.get());
            } catch (WriteException | InterruptedException | ExecutionException | TimeoutException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
