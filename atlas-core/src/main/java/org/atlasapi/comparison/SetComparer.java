package org.atlasapi.comparison;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extractor for classes from which a value representing their state can be extracted by just
 * calling <code>#toString()</code>
 */
public class SetComparer extends Comparer {

    private final ObjectComparer objectComparer;

    public SetComparer(ObjectComparer objectComparer) {
        this.objectComparer = checkNotNull(objectComparer);
    }

    @Override
    public boolean isSupported(Object object) {
        return object instanceof Set;

    }

    @Override
    public boolean equals(Object object1, Object object2) {
        Set<Object> set1 = ((Set<Object>) object1);
        Set<Object> set2 = ((Set<Object>) object2);

        if (set1.size() != set2.size()) {
            return false;
        }

        Map<Object, Object> lookup = new HashMap<>();

        for (Set<Object> set : ImmutableList.of(set1, set2)) {
            for (Object element : set) {
                Object otherElement = lookup.remove(element);

                if (otherElement == null) {
                    lookup.put(element, element);
                } else {
                    boolean isEqual = objectComparer.equals(element, otherElement);
                    if (!isEqual) {
                        return false;
                    }
                }
            }
        }

        return lookup.isEmpty();
    }
}
