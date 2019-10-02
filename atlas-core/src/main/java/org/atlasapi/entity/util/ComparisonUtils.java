package org.atlasapi.entity.util;

import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.entity.Sameable;

import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComparisonUtils {

    public static boolean isSame(
            @Nullable Sameable s1,
            @Nullable Sameable s2
    ) {
        return (s1 == s2) || (s1 != null && s1.isSame(s2));
    }

    public static boolean isSame(
            @Nullable Iterable<? extends Sameable> i1,
            @Nullable Iterable<? extends Sameable> i2
    ){
        if (i1 == i2) return true;
        if (i1 == null || i2 == null) return false;
        Iterator<? extends Sameable> it1 = i1.iterator();
        Iterator<? extends Sameable> it2 = i2.iterator();
        while(it1.hasNext() && it2.hasNext()) {
            if (!it1.next().isSame(it2.next())) {
                return false;
            }
        }
        if(it1.hasNext() || it2.hasNext()) {
            return false;
        }
        return true;
    }

    public static boolean isSame(
            @Nullable List<? extends Sameable> l1,
            @Nullable List<? extends Sameable> l2
    ){
        if (l1 == l2) return true;
        if (l1 == null || l2 == null) return false;
        if (l1.size() != l2.size()) return false;
        Iterator<? extends Sameable> it1 = l1.iterator();
        Iterator<? extends Sameable> it2 = l2.iterator();
        while(it1.hasNext() && it2.hasNext()) {
            if (!it1.next().isSame(it2.next())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSame(
            @Nullable Set<? extends Sameable> s1,
            @Nullable Set<? extends Sameable> s2
    ) {
        if (s1 == s2) return true;
        if (s1 == null || s2 == null) return false;
        if (s1.size() != s2.size()) return false;
        Set<SameableWithEquals> s1WithEquals = toSet(s1);
        Set<SameableWithEquals> s2WithEquals = toSet(s2);
        return s1WithEquals.equals(s2WithEquals);
    }

    private static Set<SameableWithEquals> toSet(Set<? extends Sameable> set) {
        return set.stream()
                .map(SameableWithEquals::new)
                .collect(MoreCollectors.toImmutableSet());
    }

    public static boolean isSame(
            @Nullable Map<? extends Sameable, Iterable<? extends Sameable>> m1,
            @Nullable Map<? extends Sameable, Iterable<? extends Sameable>> m2
    ) {
        if (m1 == m2) return true;
        if (m1 == null || m2 == null) return false;
        if (m1.size() != m2.size()) return false;
        Map<SameableWithEquals, SameablesWithEquals> m1WithEquals = toMap(m1);
        Map<SameableWithEquals, SameablesWithEquals> m2WithEquals = toMap(m2);
        return m1WithEquals.equals(m2WithEquals);
    }

    private static Map<SameableWithEquals, SameablesWithEquals> toMap(
            Map<? extends Sameable, Iterable<? extends Sameable>> map
    ) {
        return map.entrySet().stream()
                .map(entry ->
                        new AbstractMap.SimpleImmutableEntry<>(
                                new SameableWithEquals(entry.getKey()), new SameablesWithEquals(entry.getValue())
                        )
                )
                .collect(MoreCollectors.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    private static class SameableWithEquals {
        private Sameable sameable;

        private SameableWithEquals(Sameable sameable) {
            this.sameable = sameable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SameableWithEquals that = (SameableWithEquals) o;
            return this.sameable.isSame(that.sameable);
        }
    }

    private static class SameablesWithEquals {
        private Iterable<? extends Sameable> sameables;

        private SameablesWithEquals(Iterable<? extends Sameable> sameables) {
            this.sameables = sameables;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SameablesWithEquals that = (SameablesWithEquals) o;
            return isSame(this.sameables, that.sameables);
        }
    }
}
