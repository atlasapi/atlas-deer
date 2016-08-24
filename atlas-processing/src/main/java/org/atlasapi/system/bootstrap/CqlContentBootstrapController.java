package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Described;
import org.atlasapi.content.Episode;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.system.legacy.ProgressStore;

import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.elasticsearch.common.lang3.ObjectUtils;
import org.joda.time.DateTime;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
@RequestMapping("/system/bootstrap/cql-content")
public class CqlContentBootstrapController {

    private static final String TASK_NAME = "cql-mongo-id-bootstrap";
    private static final long PROGRESS_SAVE_FREQUENCY = 100L;
    private static final String KAFKA_SENDER_NAME = "cql-mondo-id-bootstrap-sender";

    private final ListeningExecutorService listeningExecutorService;
    private final ProgressStore progressStore;
    private final ResourceLister<Content> contentLister;
    private final MessageSenderFactory messageSenderFactory;
    private final MessageConsumerFactory<?> messageConsumerFactory;

    private MessageSender<ResourceUpdatedMessage> sender;

    public static CqlContentBootstrapController create(
            ListeningExecutorService listeningExecutorService,
            ProgressStore progressStore,
            ResourceLister<Content> contentLister,
            MessageSenderFactory messageSenderFactory,
            MessageConsumerFactory<?> messageConsumerFactory
    ) {
        return new CqlContentBootstrapController(
                listeningExecutorService,
                progressStore,
                contentLister,
                messageSenderFactory,
                messageConsumerFactory
        );
    }

    private CqlContentBootstrapController(
            ListeningExecutorService listeningExecutorService,
            ProgressStore progressStore,
            ResourceLister<Content> contentLister,
            MessageSenderFactory messageSenderFactory,
            MessageConsumerFactory<?> messageConsumerFactory
    ) {
        this.contentLister = checkNotNull(contentLister);
        this.listeningExecutorService = checkNotNull(listeningExecutorService);
        this.progressStore = checkNotNull(progressStore);
        this.messageSenderFactory = checkNotNull(messageSenderFactory);
        this.messageConsumerFactory = checkNotNull(messageConsumerFactory);
    }

    @RequestMapping(value = "/list-mongo-ids", method = RequestMethod.POST)
    public void migrate(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ExecutionException, InterruptedException, WriteException {
        Optional<ContentListingProgress> storedProgress = progressStore.progressForTask(TASK_NAME);
        ContentListingProgress progress = storedProgress.orElse(ContentListingProgress.START);

        initSender();

        listeningExecutorService.submit(() -> {
            long numProcessed = 0L;

            for (Content content : contentLister.list(progress)) {
                bootstrap(content);

                numProcessed++;

                storeProgress(numProcessed, content);
            }
        });

        response.setStatus(200);
        try {
            response.getWriter().write("Started message sender");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void initSender() {
        this.sender = checkNotNull(
                messageSenderFactory).makeMessageSender(
                KAFKA_SENDER_NAME,
                JacksonMessageSerializer.forType(ResourceUpdatedMessage.class)
        );
    }

    private void bootstrap(Content content) {
        if (content instanceof Episode) {
            Episode episode = (Episode) content;

            ContainerRef brand = episode.getContainerRef();
            SeriesRef series = episode.getSeriesRef();
            EpisodeRef episodeRef = episode.toRef();

            ContentRef partition = ObjectUtils.firstNonNull(brand, series, episodeRef);

            sendMessage(brand, partition);
            sendMessage(series, partition);
            sendMessage(episodeRef, partition);
        } else if (content instanceof Series) {
            Series series = (Series) content;

            BrandRef brand = series.getBrandRef();
            SeriesRef seriesRef = series.toRef();

            ContentRef partition = ObjectUtils.firstNonNull(brand, seriesRef);

            sendMessage(brand, partition);
            sendMessage(seriesRef, partition);
        } else {
            ContentRef contentRef = content.toRef();

            sendMessage(contentRef, contentRef);
        }
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
}
