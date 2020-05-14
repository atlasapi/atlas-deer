package org.atlasapi.query.v5.search.coercer;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

public class InstantRangeCoercer extends EqualLengthRangeCoercer<Instant> {
    private InstantRangeCoercer() {
        super(Instant::parse);
    }
    public static InstantRangeCoercer create() {
        return new InstantRangeCoercer();
    }
}
