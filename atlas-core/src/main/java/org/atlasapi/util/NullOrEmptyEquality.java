package org.atlasapi.util;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * This class contains utility methods to treat empty map and collection types the same as if they were null.
 * This is used within the equality and hash methods in the content_v2 serialized model classes so that they can be
 * accurately used to determine if content has changed when migrating; it seems that empty maps and collections
 * are left as null when writing to Cassandra thus requiring custom logic to treat them as the same.
 */
public class NullOrEmptyEquality {

    public static boolean equals(@Nullable Collection collection1, @Nullable Collection collection2) {
        if ((collection1 == null || collection1.isEmpty()) && (collection2 == null || collection2.isEmpty())) {
            return true;
        }
        return Objects.equals(collection1, collection2);
    }

    public static boolean equals(@Nullable Map map1, @Nullable Map map2) {
        if ((map1 == null || map1.isEmpty()) && (map2 == null || map2.isEmpty())) {
            return true;
        }
        return Objects.equals(map1, map2);
    }

    public static int hash(Object... objects) {
        for (int i = 0; i < objects.length; i++) {
            Object object = objects[i];
            if(object instanceof Collection) {
                Collection collection = (Collection) object;
                objects[i] = collection.isEmpty() ? null : collection;
            } else if (object instanceof Map) {
                Map map = (Map) object;
                objects[i] = map.isEmpty() ? null : map;
            }
        }
        return Objects.hash(objects);
    }
}
