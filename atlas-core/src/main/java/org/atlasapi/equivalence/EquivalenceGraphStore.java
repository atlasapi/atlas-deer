package org.atlasapi.equivalence;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

/**
 * <p> Records equivalences between resources. </p>
 */
public interface EquivalenceGraphStore {

    /**
     * <p> Record a subject as equivalent to a set of other resources. </p>
     * <p>
     * <p> An update overwrites all equivalences for resources which are sourced from elements of
     * the supplied sources set. </p>
     * <p>
     * <p> The return value, if an update occurs, is the set of new {@link EquivalenceGraph}s formed
     * as a result of the update. </p>
     * <p>
     * <p> Updates are idempotent - if the subject is asserted equivalent to the same resources and
     * set of sources then no update occurs and <code>Optional.absent()</code> is returned. </p>
     * <p>
     * <p> An update may not occur if the update would create a equivalence set that is too large.
     * </p>
     *
     * @param subject           - reference to the resource which is to be recorded equivalent to
     *                          the asserted adjacent resources.
     * @param assertedAdjacents - references to resources which are equivalent to the subject.
     * @param sources           - the {@link org.atlasapi.media.entity.Publisher Publisher}s for
     *                          which this update should be applied.
     * @return - an {@link EquivalenceGraphUpdate} resulting from this update, or
     * <code>Optional.absent()</code> if no update occurs.
     * @throws WriteException - if there is an exception recording the update of equivalences.
     */
    Optional<EquivalenceGraphUpdate> updateEquivalences(ResourceRef subject,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) throws WriteException;

    Optional<EquivalenceGraphUpdate> forceUpdateEquivalences(
            ResourceRef subject,
            Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources
    ) throws WriteException;

    ListenableFuture<OptionalMap<Id, EquivalenceGraph>> resolveIds(Iterable<Id> ids);

}
