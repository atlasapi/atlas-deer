package org.atlasapi.query.v5.search;

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

    @Override
    protected Instant[] bisectAndCoerce(String value) throws InvalidAttributeValueException {
        if (value.length() % 2 != 1) { // Instant range values must be in the same format
            throw new InvalidAttributeValueException(value);
        }

        int mid = (value.length()-1)/2;
        String from = value.substring(0, mid);
        String to = value.substring(mid+1);

        try {
           return new Instant[] { coercerFunction.apply(from), coercerFunction.apply(to) };
        } catch (DateTimeParseException e) {
           throw new InvalidAttributeValueException(value);
        }
    }
}
