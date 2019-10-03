package org.atlasapi.comparison;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extractor for classes from which a value representing their state can be extracted by just
 * calling <code>#toString()</code>
 */
public class MapComparer extends Comparer {

    private final ObjectComparer objectComparer;

    public MapComparer(ObjectComparer objectComparer) {
        this.objectComparer = checkNotNull(objectComparer);
    }

    @Override
    public boolean isSupported(Object object) {
        return object instanceof Map;

    }

    @Override
    public boolean equals(Object object1, Object object2) {
        Map<Object, Object> map1 = ((Map<Object, Object>) object1);
        Map<Object, Object> map2 = ((Map<Object, Object>) object2);

        if (map1.size() != map2.size()) {
            return false;
        }

        for (Map.Entry<Object, Object> entry : map1.entrySet()) {
            Object value2 = map2.get(entry.getKey());
            if (value2 == null) {
                return false;
            }
            boolean isEqual = objectComparer.equals(entry.getValue(), value2);
            if (!isEqual) {
                return  false;
            }
        }

        return true;
    }
}
