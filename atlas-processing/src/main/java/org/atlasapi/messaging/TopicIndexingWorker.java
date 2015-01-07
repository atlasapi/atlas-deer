package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.IndexException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicIndex;
import org.atlasapi.topic.TopicResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class TopicIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TopicResolver topicResolver;
    private final TopicIndex topicIndex;

    public TopicIndexingWorker(TopicResolver topicResolver, TopicIndex topicIndex) {
        this.topicResolver = topicResolver;
        this.topicIndex = topicIndex;
    }

    @Override
    public void process(final ResourceUpdatedMessage message) {
        try {
            Resolved<Topic> results = Futures.get(resolveContent(message), 1, TimeUnit.MINUTES, TimeoutException.class);
            Optional<Topic> topic = results.getResources().first();
            if (topic.isPresent()) {
                Topic source = topic.get();
                log.debug("indexing {}", source);
                topicIndex.index(source);
            } else {
                log.warn("{}: failed to resolve {} ",
                        new Object[]{message.getMessageId(), message.getUpdatedResource()});
            }
        } catch (TimeoutException | IndexException e) {
            log.error("iqqndexing error:", e);
        }
    }

    private ListenableFuture<Resolved<Topic>> resolveContent(final ResourceUpdatedMessage message) {
        return topicResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }
}