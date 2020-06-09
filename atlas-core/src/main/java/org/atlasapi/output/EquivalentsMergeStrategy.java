package org.atlasapi.output;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.equivalence.Equivalable;

/**
 * Merges a set of equivalents into a single chosen resource.
 *
 * @param <E>
 */
public interface EquivalentsMergeStrategy<E extends Equivalable<E>> {

    /**
     * @param equivalents - a set of resources equivalent to chosen.
     * @param activeAnnotations - the annotations present on the call
     * @return merged resources.
     */
    <T extends E> T merge(Iterable<? extends T> equivalents, Application application,
            Set<Annotation> activeAnnotations);
}
