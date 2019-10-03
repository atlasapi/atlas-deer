package org.atlasapi.comparison;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extractor for classes from which a value representing their state can be extracted by just
 * calling <code>#toString()</code>
 */
public class ListComparer extends Comparer {

    private final IterableComparer iterableComparer;

    public ListComparer(IterableComparer iterableComparer) {
        this.iterableComparer = checkNotNull(iterableComparer);
    }

    @Override
    public boolean isSupported(Object object) {
        return object instanceof List;

    }

    @Override
    public boolean equals(Object object1, Object object2) {
        List<Object> list1 = ((List<Object>) object1);
        List<Object> list2 = ((List<Object>) object2);

        if (list1.size() != list2.size()) {
            return false;
        }

        return iterableComparer.equals(list1, list2);
    }
}
