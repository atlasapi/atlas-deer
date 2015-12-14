package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;


public class TopicReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    public static final Logger LOG = LoggerFactory.getLogger(TopicReadWriteWorker.class);

    private final TopicResolver resolver;
    private final TopicWriter writer;
    private final Timer metricsTimer;

    public TopicReadWriteWorker(TopicResolver resolver, TopicWriter writer, Timer metricsTimer) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.metricsTimer = checkNotNull(metricsTimer);
    }
    
    @Override
    public void process(ResourceUpdatedMessage message) {
        LOG.debug("Processing message on id {}, message: {}",
                message.getUpdatedResource().getId(), message);

        Timer.Context time = metricsTimer.time();

        ImmutableList<Id> ids = ImmutableList.of(message.getUpdatedResource().getId());
        ListenableFuture<Resolved<Topic>> read = resolver.resolveIds(ids);
        Futures.addCallback(read, new FutureCallback<Resolved<Topic>>() {

            @Override
            public void onSuccess(Resolved<Topic> result) {
                for (Topic topic : result.getResources()) {
                    writer.writeTopic(topic);
                }
                time.stop();
            }

            @Override
            public void onFailure(Throwable t) {
                time.stop();
                throw Throwables.propagate(t);
            }

        });
    }

}
