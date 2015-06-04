package org.atlasapi.util;

import java.util.function.Predicate;

public class MorePredicates {

    public static <T> Predicate<T> isNotNull() {
        return t1 -> t1 != null;
    }
}
