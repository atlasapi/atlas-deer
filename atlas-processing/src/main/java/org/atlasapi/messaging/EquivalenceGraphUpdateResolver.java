package org.atlasapi.messaging;

import java.util.concurrent.ExecutionException;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.util.ImmutableCollectors;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The purpose of this class it to make
 * {@link org.atlasapi.equivalence.EquivalenceGraphUpdateMessage} messages idempotent by
 * resolving the referenced graphs from the {@link org.atlasapi.equivalence.EquivalenceGraphStore}
 * and updating the message to contain an up-to-date view of the system.
 * <p>
 * This accomplishes:
 * <ul>
 *     <li>Allows us to replay messages as needed to cope with a failure</li>
 *     <li>Eliminates issues where a backlogged queue will not apply updates that represent
 *     an out-of-date view of the system when processing old messages</li>
 *     <li>Allows us to retry messages out of order if they fail</li>
 * </ul>
 * <p>
 * This class can only be used when the updated/created graphs can be treated identically and the
 * deleted graphs ignored, i.e. if the only part of the message that the consumer intends to use
 * is the output of {@link EquivalenceGraphUpdate#getAllGraphs()}.
 */
public class EquivalenceGraphUpdateResolver {

    private static final Logger log = LoggerFactory.getLogger(EquivalenceGraphUpdateResolver.class);

    private final EquivalenceGraphStore graphStore;

    private EquivalenceGraphUpdateResolver(EquivalenceGraphStore graphStore) {
        this.graphStore = checkNotNull(graphStore);
    }

    public static EquivalenceGraphUpdateResolver create(EquivalenceGraphStore graphStore) {
        return new EquivalenceGraphUpdateResolver(graphStore);
    }

    public ImmutableSet<EquivalenceGraph> resolve(EquivalenceGraphUpdate graphUpdate) {
        ImmutableSet<Id> graphIds = graphUpdate.getAllGraphs()
                .stream()
                .map(EquivalenceGraph::getId)
                .collect(ImmutableCollectors.toSet());

        try {
            return graphStore.resolveIds(graphIds).get()
                    .values()
                    .stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(ImmutableCollectors.toSet());

        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to resolve equivalence graphs for {}", Joiner.on(",").join(graphIds));
            throw Throwables.propagate(e);
        }
    }
}
