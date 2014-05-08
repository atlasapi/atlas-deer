package org.atlasapi.content;

import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

public enum Specialization {

    TV,
    RADIO,
    MUSIC,
    FILM;
    private static final Splitter CSV_SPLITTER = Splitter.on(',').trimResults();

    public static Maybe<Specialization> fromKey(String key) {
        for (Specialization s : Specialization.values()) {
            if (key.toLowerCase().equals(s.toString())) {
                return Maybe.just(s);
            }
        }
        return Maybe.nothing();
    }

    public static Iterable<Specialization> fromCsv(String csv) {
        Iterable<String> splits = CSV_SPLITTER.split(csv);
        Set<Specialization> result = Sets.newHashSet();
        for (String candidate : splits) {
            for (Specialization specialization : Specialization.values()) {
                if (candidate.toLowerCase().equals(specialization.toString())) {
                    result.add(specialization);
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
