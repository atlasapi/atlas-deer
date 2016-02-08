package org.atlasapi.messaging;

import javax.annotation.Nullable;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdatingWorker implements Worker<EquivalenceAssertionMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContentEquivalenceUpdatingWorker.class);

    private final EquivalenceGraphStore graphStore;
    private final DirectAndExplicitEquivalenceMigrator equivMigrator;
    @Nullable private final Timer messageTimer;
    @Nullable private final Meter meter;

    public ContentEquivalenceUpdatingWorker(EquivalenceGraphStore graphStore,
            @Nullable MetricRegistry metrics,
            DirectAndExplicitEquivalenceMigrator equivMigrator) {
        this.graphStore = checkNotNull(graphStore);
        this.messageTimer = (metrics != null ?
                             checkNotNull(metrics.timer("ContentEquivalenceUpdatingWorker")) :
                             null);
        this.meter = (metrics != null ?
                      checkNotNull(metrics.meter("ContentEquivalenceUpdatingWorker.errorRate")) :
                      null);
        this.equivMigrator = checkNotNull(equivMigrator);
    }

    @Override
    public void process(EquivalenceAssertionMessage message) throws RecoverableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processing message on id {}, asserted adjacents: {}, message: {}",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(ImmutableCollectors.toList()),
                    message
            );
        }

        try {
            if (messageTimer != null) {
                Timer.Context timer = messageTimer.time();
                graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(),
                        message.getPublishers()
                );
                equivMigrator.migrateEquivalence(message.getSubject());
                timer.stop();
            } else {
                graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(),
                        message.getPublishers()
                );
                equivMigrator.migrateEquivalence(message.getSubject());
            }
            LOG.debug("Successfully processed message {}", message.toString());
        } catch (WriteException e) {
            LOG.warn(
                    "Failed to process message on id {}, asserted adjacents: {}, message: {}. "
                            + "Retrying...",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(ImmutableCollectors.toList()),
                    message
            );
            throw new RecoverableException("update failed for " + message.toString(), e);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to process message on id {}, asserted adjacents: {}, message: {}",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(ImmutableCollectors.toList()),
                    message
            );
            if (meter != null) {
                meter.mark();
            }
        }
    }
}
