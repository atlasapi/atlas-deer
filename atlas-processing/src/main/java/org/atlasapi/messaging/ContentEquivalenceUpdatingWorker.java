package org.atlasapi.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdatingWorker implements Worker<EquivalenceAssertionMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContentEquivalenceUpdatingWorker.class);

    private final EquivalenceGraphStore graphStore;
    private final DirectAndExplicitEquivalenceMigrator equivMigrator;

    private final MetricRegistry metricRegistry;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final String publisherMeterName;

    private ContentEquivalenceUpdatingWorker(
            EquivalenceGraphStore graphStore,
            DirectAndExplicitEquivalenceMigrator equivMigrator,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.graphStore = checkNotNull(graphStore);
        this.equivMigrator = checkNotNull(equivMigrator);

        this.metricRegistry = metricRegistry;

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");

        this.publisherMeterName = metricPrefix + "source.%s.meter.received";
    }

    public static ContentEquivalenceUpdatingWorker create(
            EquivalenceGraphStore graphStore,
            DirectAndExplicitEquivalenceMigrator equivMigrator,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new ContentEquivalenceUpdatingWorker(
                graphStore, equivMigrator, metricPrefix, metricRegistry
        );
    }

    private static final ArrayList<Long> BAD_DOGS =
            new ArrayList<>(Arrays.asList(
                    95116171L,
                    111452500L,
                    111647766L,
                    109125367L,
                    95212972L,
                    95126530L,
                    95187770L,
                    94973539L,
                    95507473L,
                    95436469L,
                    109546816L,
                    124058646L,
                    124132921L,
                    130668126L,
                    109536940L,
                    130647967L,
                    131563936L,
                    124124570L,
                    124133377L,
                    124101253L,
                    124124662L,
                    124132364L,
                    124131284L,
                    124058738L,
                    128050993L));
    @Override
    public void process(EquivalenceAssertionMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        if(BAD_DOGS.contains(message.getSubject().getId().longValue()){
            return;
        }
        metricRegistry.meter(
                String.format(
                        publisherMeterName,
                        message.getSubject().getSource().key().replace('.', '_')
                ))
                .mark();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processing message on id {}, took: PT{}S, asserted adjacents: {}, message: {}",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(MoreCollectors.toImmutableList()),
                    getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                    message
            );
        }

        Timer.Context time = executionTimer.time();

        try {
            graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(),
                    message.getPublishers()
            );
            equivMigrator.migrateEquivalence(message.getSubject());

            latencyTimer.update(
                    getTimeToProcessInMillis(message.getTimestamp()),
                    TimeUnit.MILLISECONDS
            );
            LOG.debug("Successfully processed message {}", message.toString());
        } catch (Exception e) {
            LOG.warn(
                    "Failed to process message on id {}, asserted adjacents: {}, message: {}",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(MoreCollectors.toImmutableList()),
                    message
            );
            failureMeter.mark();

            throw Throwables.propagate(e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
