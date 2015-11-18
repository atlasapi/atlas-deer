package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.atlasapi.content.IndexException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicIndex;
import org.atlasapi.topic.TopicResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class TopicIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(TopicIndexingWorker.class);

    private final TopicResolver topicResolver;
    private final TopicIndex topicIndex;
    private final Timer messageTimer;

    public TopicIndexingWorker(TopicResolver topicResolver, TopicIndex topicIndex,
                               @Nullable MetricRegistry metrics) {
        this.topicResolver = topicResolver;
        this.topicIndex = topicIndex;
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("TopicIndexingWorker")) : null);
    }

    @Override
    public void process(final ResourceUpdatedMessage message) throws RecoverableException {
        LOG.debug("Processing message on id {}, message: {}", message.getUpdatedResource(), message);

        try {
            Timer.Context time = null;
            if (messageTimer != null) {
                time = messageTimer.time();
            }
            Resolved<Topic> results = Futures.get(resolveContent(message), 1, TimeUnit.MINUTES, TimeoutException.class);
            Optional<Topic> topic = results.getResources().first();
            if (topic.isPresent()) {
                Topic source = topic.get();
                LOG.debug("indexing {}", source);
                topicIndex.index(source);
            } else {
                LOG.warn("{}: failed to resolve {} ",
                        new Object[]{message.getMessageId(), message.getUpdatedResource()});
            }
            if (time != null) {
                time.stop();
            }
        } catch (TimeoutException | IndexException e) {
            throw new RecoverableException("indexing error:", e);
        }
    }

    private ListenableFuture<Resolved<Topic>> resolveContent(final ResourceUpdatedMessage message) {
        return topicResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }
}