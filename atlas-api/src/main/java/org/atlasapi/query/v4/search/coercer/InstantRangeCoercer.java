package org.atlasapi.query.v4.search.coercer;

import java.time.Instant;

public class InstantRangeCoercer extends EqualLengthRangeCoercer<Instant> {
    private InstantRangeCoercer() {
        super(Instant::parse);
    }
    public static InstantRangeCoercer create() {
        return new InstantRangeCoercer();
    }
}
