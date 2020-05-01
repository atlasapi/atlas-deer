package org.atlasapi.query.v5.search.coercer;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

public class InstantRangeCoercer extends RangeCoercer<Instant> {

    private InstantRangeCoercer() {
        super(Instant::parse);
    }

    public static InstantRangeCoercer create() {
        return new InstantRangeCoercer();
    }

    // Only accepts values containing two Instants of equal length separated by the RANGE_SEPARATOR.
    @Override
    protected Instant[] bisectAndCoerce(String value) throws InvalidAttributeValueException {

        int len = value.length();
        if (len < 41) {
            throw new InvalidAttributeValueException(value);
        }

        int mid = (len-1)/2;

        if (len % 2 != 1 || value.charAt(mid) != RANGE_SEPARATOR_CHAR) {
            throw new InvalidAttributeValueException(value);
        }

        String from = value.substring(0, mid);
        String to = value.substring(mid+1);

        try {
           return new Instant[] {
                   coercerFunction.apply(from),
                   coercerFunction.apply(to)
           };
        } catch (DateTimeParseException e) {
           throw new InvalidAttributeValueException(value);
        }
    }
}
