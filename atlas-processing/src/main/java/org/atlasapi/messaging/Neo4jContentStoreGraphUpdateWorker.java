package org.atlasapi.messaging;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceAssertion;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.Neo4jContentStore;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jContentStoreGraphUpdateWorker
        implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger log =
            LoggerFactory.getLogger(Neo4jContentStoreGraphUpdateWorker.class);

    private final Neo4jContentStore neo4JContentStore;
    private final Timer timer;
    private final Meter failureMeter;

    private Neo4jContentStoreGraphUpdateWorker(
            Neo4jContentStore neo4JContentStore,
            Timer timer,
            Meter failureMeter
    ) {
        this.neo4JContentStore = checkNotNull(neo4JContentStore);
        this.timer = checkNotNull(timer);
        this.failureMeter = checkNotNull(failureMeter);
    }

    public static Neo4jContentStoreGraphUpdateWorker create(
            Neo4jContentStore neo4JContentStore,
            Timer timer,
            Meter failureMeter
    ) {
        return new Neo4jContentStoreGraphUpdateWorker(
                neo4JContentStore, timer, failureMeter
        );
    }

    // We can't remove the deprecated exception from the method description because it's on the
    // interface.
    @SuppressWarnings("deprecation")
    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        EquivalenceAssertion assertion = message.getGraphUpdate().getAssertion();

        if (assertion == null) {
            // This should only happen with messages that were sent before this commit
            // was put live.
            log.warn("Received message with no equivalence assertion");
            return;
        }

        log.debug(
                "Processing message on subject: {}, took: PT{}S, "
                        + "asserted adjacents: {}, asserted sources: {}, message: {}",
                assertion.getSubject().getId(),
                getTimeToProcessInSeconds(message),
                assertion.getAssertedAdjacents().stream()
                        .map(ResourceRef::getId)
                        .collect(MoreCollectors.toImmutableSet()),
                assertion.getSources().stream()
                        .map(Publisher::key)
                        .collect(MoreCollectors.toImmutableSet())
        );

        Timer.Context time = timer.time();

        try {
            neo4JContentStore.writeEquivalences(
                    assertion.getSubject(),
                    assertion.getAssertedAdjacents(),
                    assertion.getSources()
            );

            time.stop();
        } catch (Exception e) {
            failureMeter.mark();
            throw Throwables.propagate(e);
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
