package org.atlasapi.query.v5.search.coercer;

import java.util.function.Function;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

public class EqualLengthRangeCoercer<T> extends RangeCoercer<T> {

    protected EqualLengthRangeCoercer(Function<String, T> coercerFunction) {
        super(coercerFunction);
    }

    @Override
    protected Range<T> bisectAndCoerce(String value) throws InvalidAttributeValueException {

        int len = value.length();
        if (len < 3) {
            throw new InvalidAttributeValueException(value);
        }

        int mid = (len-1)/2;

        if (len % 2 != 1 || value.charAt(mid) != RANGE_SEPARATOR_CHAR) {
            throw new InvalidAttributeValueException(value);
        }

        String from = value.substring(0, mid);
        String to = value.substring(mid+1);

        try {
           return new Range<>(
                   coercerFunction.apply(from),
                   coercerFunction.apply(to)
           );
        } catch (Exception e) {
           throw new InvalidAttributeValueException(value);
        }
    }
}
