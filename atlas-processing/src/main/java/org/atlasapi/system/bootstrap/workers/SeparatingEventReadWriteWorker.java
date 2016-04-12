package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.eventV2.EventV2Writer;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeparatingEventReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(SeparatingEventReadWriteWorker.class);

    private final EventV2Resolver resolver;
    private final EventV2Writer writer;
    private final Timer metricsTimer;

    public SeparatingEventReadWriteWorker(EventV2Resolver resolver, EventV2Writer writer, Timer metricsTimer) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.metricsTimer = checkNotNull(metricsTimer);
    }

    @Override
    public void process(ResourceUpdatedMessage message)
            throws RecoverableException {
        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(), getTimeToProcessInSeconds(message), message
        );

        ImmutableList<Id> ids = ImmutableList.of(message.getUpdatedResource().getId());
        process(ids);
    }

    public void process(Iterable<Id> ids) {
        Timer.Context time = metricsTimer.time();

        ListenableFuture<Resolved<EventV2>> future = resolver.resolveIds(ids);
        Futures.addCallback(future, new FutureCallback<Resolved<EventV2>>() {

            @Override
            public void onSuccess(Resolved<EventV2> result) {
                for (EventV2 event : result.getResources()) {
                    try {
                        writer.write(event);
                    } catch (WriteException e) {
                        LOG.warn("Failed to write event " + event.getId());
                    } finally {
                        time.stop();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                time.stop();
                throw Throwables.propagate(t);
            }
        });
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
