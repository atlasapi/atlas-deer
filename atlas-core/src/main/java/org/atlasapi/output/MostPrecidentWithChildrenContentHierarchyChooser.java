package org.atlasapi.output;

import org.atlasapi.content.Container;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * A {@link EquivalentSetContentHierarchyChooser} that chooses the first 
 * {@link Container}, according to the order provided, with children will 
 * be chosen.
 *
 */
public class MostPrecidentWithChildrenContentHierarchyChooser implements EquivalentSetContentHierarchyChooser {

    @Override
    public Optional<Container> chooseBestHierarchy(Iterable<Container> containers) {
        return Iterables.tryFind(containers,
                                 Predicates.and(
                                         Predicates.notNull(),
                                         (Container input) -> input.getItemRefs() != null &&
                                                              !input.getItemRefs().isEmpty())
                                );
    }

}
