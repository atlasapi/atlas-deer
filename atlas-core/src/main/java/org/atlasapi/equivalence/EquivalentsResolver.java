package org.atlasapi.equivalence;

import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A content resolver which returns the equivalent set for each requested
 * identifiers inside the given set of sources.
 * 
 */
public interface EquivalentsResolver<E extends Equivalable<E>> {

    /**
     * Resolves the equivalent sets of content for a given set of source URIs.
     * Only content from the given sources is resolved.
     * 
     * @param ids
     *            - requested numeric keys of equivalent content.
     * @param selectedSources
     *            - sources of the equivalent set to resolve.
     * @param activeAnnotations
     *            - components of the model to resolve.
     * @return EquivalentContent with an entry for each of the requested IDs.
     */
    ListenableFuture<ResolvedEquivalents<E>> resolveIds(Iterable<Id> ids, Set<Publisher> selectedSources, Set<Annotation> activeAnnotations);

    ListenableFuture<ResolvedEquivalents<E>> resolveIdsWithoutEquivalence(Iterable<Id> ids, Set<Publisher> selectedSources, Set<Annotation> activeAnnotations);


}
