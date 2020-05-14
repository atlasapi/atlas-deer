package org.atlasapi.query.v5.search.coercer;

import java.time.LocalDate;

public class DateRangeCoercer extends EqualLengthRangeCoercer<LocalDate> {
    private DateRangeCoercer() {
        super(LocalDate::parse);
    }
    public static DateRangeCoercer create() {
        return new DateRangeCoercer();
    }
}
