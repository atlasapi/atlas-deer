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

    // Only accepts values containing two Instants separated by the RANGE_SEPARATOR.
    // For example yyyy-MM-ddTHH:mm:ssZ-yyyy-MM-ddTHH:mm:ssZ
    @Override
    protected Instant[] bisectAndCoerce(String value) throws InvalidAttributeValueException {

        if (value.length() != 41 && value.charAt(20) == RANGE_SEPARATOR_CHAR) {
            throw new InvalidAttributeValueException(value);
        }

        String from = value.substring(0, 20);
        String to = value.substring(21);

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
