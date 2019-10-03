package org.atlasapi.comparison;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extractor for classes from which a value representing their state can be extracted by just
 * calling <code>#toString()</code>
 */
public class IterableComparer extends Comparer {

    private final ObjectComparer objectComparer;

    public IterableComparer(ObjectComparer objectComparer) {
        this.objectComparer = checkNotNull(objectComparer);
    }

    @Override
    public boolean isSupported(Object object) {
        return object instanceof Iterable;

    }

    @Override
    public boolean equals(Object object1, Object object2) {
        Iterator<Object> iterator1 = ((Iterable<Object>) object1).iterator();
        Iterator<Object> iterator2 = ((Iterable<Object>) object2).iterator();

        while (iterator1.hasNext() && iterator2.hasNext()) {
            Object o1 = iterator1.next();
            Object o2 = iterator2.next();
            boolean objectsEqual = objectComparer.equals(o1, o2);
            if (!objectsEqual) {
                return false;
            }
        }

        return !(iterator1.hasNext() || iterator2.hasNext());
    }
}
