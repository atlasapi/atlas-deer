package org.atlasapi.output;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Container;
import org.atlasapi.content.Series;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link EquivalentSetContentHierarchyChooser} that chooses the first {@link Container} with a
 * true brand/series/episode (or series/episode in the case of top-level series) hierarchy. If none
 * is found, the order will be delegated to the provided fall-back {@link
 * EquivalentSetContentHierarchyChooser}
 */
public class FirstHierarchyContentHierarchyChooser implements EquivalentSetContentHierarchyChooser {

    private final EquivalentSetContentHierarchyChooser fallback;

    public FirstHierarchyContentHierarchyChooser(EquivalentSetContentHierarchyChooser fallback) {
        this.fallback = checkNotNull(fallback);
    }

    @Override
    public Optional<Container> chooseBestHierarchy(Iterable<Container> containers) {
        Optional<Container> best = Iterables.tryFind(containers, HAS_HIERARCHY);

        if (best.isPresent()) {
            return best;
        }

        return fallback.chooseBestHierarchy(containers);
    }

    private static final Predicate<Container> HAS_HIERARCHY = new Predicate<Container>() {

        @Override
        public boolean apply(Container input) {
            if (input == null) {
                return false;
            }
            if (input instanceof Series) {
                Series series = (Series) input;
                return series.getItemRefs() != null
                        && !series.getItemRefs().isEmpty();
            }
            if (input instanceof Brand) {
                Brand brand = (Brand) input;
                return brand.getSeriesRefs() != null
                        && !brand.getSeriesRefs().isEmpty();
            }
            throw new IllegalArgumentException("Container of type "
                    + input.getClass().getName()
                    + " needs to be supported");
        }

    };

}
