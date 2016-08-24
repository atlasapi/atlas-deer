package org.atlasapi.messaging;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceAssertion;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jContentStoreGraphUpdateWorker
        implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger log =
            LoggerFactory.getLogger(Neo4jContentStoreGraphUpdateWorker.class);

    private final ContentResolver legacyResolver;
    private final LookupEntryStore legacyEquivalenceStore;
    private final Neo4jContentStore neo4JContentStore;
    private final Timer timer;
    private final Meter failureMeter;

    private Neo4jContentStoreGraphUpdateWorker(
            ContentResolver legacyResolver,
            LookupEntryStore legacyEquivalenceStore,
            Neo4jContentStore neo4JContentStore,
            Timer timer,
            Meter failureMeter
    ) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.legacyEquivalenceStore = checkNotNull(legacyEquivalenceStore);
        this.neo4JContentStore = checkNotNull(neo4JContentStore);
        this.timer = checkNotNull(timer);
        this.failureMeter = checkNotNull(failureMeter);
    }

    public static Neo4jContentStoreGraphUpdateWorker create(
            ContentResolver legacyResolver,
            LookupEntryStore legacyEquivalenceStore,
            Neo4jContentStore neo4JContentStore,
            Timer timer,
            Meter failureMeter
    ) {
        return new Neo4jContentStoreGraphUpdateWorker(
                legacyResolver,
                legacyEquivalenceStore,
                neo4JContentStore,
                timer,
                failureMeter
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
                        .collect(MoreCollectors.toImmutableSet()),
                message
        );

        Timer.Context time = timer.time();

        try {
            ImmutableSet<ResourceRef> adjacents = getAdjacents(
                    assertion.getSubject()
            );

            // Since we are resolving the lookup entry from Mongo this assertion is on all
            // sources, not just those in the message
            neo4JContentStore.writeEquivalences(
                    assertion.getSubject(),
                    adjacents,
                    Publisher.all()
            );

            time.stop();
        } catch (Exception e) {
            failureMeter.mark();
            throw Throwables.propagate(e);
        }
    }

    private ImmutableSet<ResourceRef> getAdjacents(
            ResourceRef subject
    ) {
        LookupEntry lookupEntry = getLookupEntry(subject.getId());
        return getAdjacents(lookupEntry);
    }

    private LookupEntry getLookupEntry(Id id) {
        return Iterables.getOnlyElement(
                legacyEquivalenceStore.entriesForIds(ImmutableList.of(id.longValue()))
        );
    }

    private ImmutableSet<ResourceRef> getAdjacents(LookupEntry lookupEntry) {
        ImmutableSet<Id> adjacentIds = Sets.union(
                lookupEntry.explicitEquivalents(),
                lookupEntry.directEquivalents()
        )
                .stream()
                .map(LookupRef::id)
                .map(Id::valueOf)
                .collect(MoreCollectors.toImmutableSet());

        return Futures.getUnchecked(
                legacyResolver.resolveIds(adjacentIds)
        )
                .getResources()
                .toList()
                .stream()
                .map(Content::toRef)
                .collect(MoreCollectors.toImmutableSet());
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
