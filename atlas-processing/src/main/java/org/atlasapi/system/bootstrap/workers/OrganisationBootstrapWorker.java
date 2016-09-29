package org.atlasapi.system.bootstrap.workers;

import java.util.concurrent.ExecutionException;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationWriter;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OrganisationBootstrapWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ContentBootstrapWorker.class);

    private final OrganisationResolver resolver;
    private final OrganisationWriter writer;
    private final Timer messagesTimer;

    public OrganisationBootstrapWorker(OrganisationResolver resolver, OrganisationWriter writer,
            Timer timer) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.messagesTimer = checkNotNull(timer);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(), getTimeToProcessInSeconds(message), message
        );

        try {
            Timer.Context time = messagesTimer.time();
            Id contentId = message.getUpdatedResource().getId();
            Resolved<Organisation> content = Futures.get(
                    resolver.resolveIds(ImmutableList.of(contentId)),
                    ExecutionException.class
            );
            Organisation organisation = content.getResources().first().get();
            writer.write(organisation);
            LOG.debug("Bootstrapped organisation {}", organisation.toString());
            time.stop();
        } catch (Exception e) {
            LOG.error(
                    "Failed to bootstrap organisation {} - {} {}",
                    message.getUpdatedResource(),
                    e,
                    Throwables
                            .getStackTraceAsString(e)
            );
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
