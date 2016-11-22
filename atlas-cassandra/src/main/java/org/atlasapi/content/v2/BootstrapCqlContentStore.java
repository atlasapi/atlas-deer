package org.atlasapi.content.v2;

import org.atlasapi.content.Content;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.hashing.content.ContentHasher;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;

public class BootstrapCqlContentStore extends CqlContentStore {

    private BootstrapCqlContentStore(
            Session session,
            MessageSender<ResourceUpdatedMessage> sender,
            IdGenerator idGenerator,
            Clock clock,
            ContentHasher hasher,
            EquivalenceGraphStore graphStore,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        super(
                session,
                sender,
                idGenerator,
                clock,
                hasher,
                graphStore,
                metricRegistry,
                metricPrefix
        );
    }

    public static BootstrapCqlContentStore create(
            Session session,
            MessageSender<ResourceUpdatedMessage> sender,
            IdGenerator idGenerator,
            Clock clock,
            ContentHasher hasher,
            EquivalenceGraphStore graphStore,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        return new BootstrapCqlContentStore(
                session,
                sender,
                idGenerator,
                clock,
                hasher,
                graphStore,
                metricRegistry,
                metricPrefix
        );
    }

    @Override
    protected Content resolvePrevious(Content content) throws WriteException {
        return null;
    }

    @Override
    protected void sendMessages(ImmutableList<ResourceUpdatedMessage> messages) {}
}
