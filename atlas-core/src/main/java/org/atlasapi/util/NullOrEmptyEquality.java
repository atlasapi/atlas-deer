package org.atlasapi.util;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

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
}
