package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationWriter;
import org.atlasapi.system.legacy.LegacyOrganisationResolver;
import org.atlasapi.system.legacy.LegacyOrganisationTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.Worker;

public class OrganisationBootstrapWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(ContentBootstrapWorker.class);

    private final OrganisationResolver resolver;
    private final OrganisationWriter writer;
    private final Timer messagesTimer;

    public OrganisationBootstrapWorker(OrganisationResolver resolver,OrganisationWriter writer, MetricRegistry metricsRegistry) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.messagesTimer = (metricsRegistry != null ? checkNotNull(metricsRegistry.timer("OrganisationBootstrapWorker")) : null);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        try {
            Timer.Context time = messagesTimer.time();
            Id contentId = message.getUpdatedResource().getId();
            Resolved<Organisation> content = Futures.get(
                    resolver.resolveIds(ImmutableList.of(contentId)),
                    ExecutionException.class
            );
            Organisation organisation = content.getResources().first().get();
            writer.write(organisation);
            log.debug("Bootstrapped organisation {}", organisation.toString());
            time.stop();
        } catch (Exception e) {
            log.error("Failed to bootstrap organisation {} - {} {}", message.getUpdatedResource(), e, Throwables
                    .getStackTraceAsString(e));
        }
    }
}
