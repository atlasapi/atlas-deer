package org.atlasapi.output;

import org.atlasapi.content.Container;

import com.google.common.base.Optional;

/**
 * Strategy for selecting which container in an equivalent set should
 * be used for series and items
 *
 */
public interface EquivalentSetContentHierarchyChooser {

    /**
     * Select {@link Container} whose children should be used. 
     * 
     * @param containers    Containers in the equivalent set, ordered 
     *                      by precedence
     * @return              Chosen container, if one is suitable.
     */
    Optional<Container> chooseBestHierarchy(Iterable<Container> containers);
}
