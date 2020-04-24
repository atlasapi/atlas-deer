package org.atlasapi.query.v5.search;

import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;

public class RangeOrTerm<T> {

    private final RangeParameter<?> range;
    private final TermParameter term;
    private final Class<T> clazz;

    private RangeOrTerm(RangeParameter<?> range, TermParameter term, Class<T> clazz) {
        this.range = range;
        this.term = term;
        if (range == null && term == null) {
            throw new IllegalArgumentException("You must provide either a Range parameter or a Term parameter.");
        }
        this.clazz = clazz;
    }

    public static RangeOrTerm<RangeParameter> of(RangeParameter<?> range) {
        return new RangeOrTerm<>(range, null, RangeParameter.class);
    }

    public static RangeOrTerm<TermParameter> of(TermParameter term) {
        return new RangeOrTerm<>(null, term, TermParameter.class);
    }

    public Class<T> getRangeOrTermClass() {
        return clazz;
    }

    public RangeParameter<?> getRange() {
        return range;
    }

    public TermParameter getTerm() {
        return term;
    }
}
