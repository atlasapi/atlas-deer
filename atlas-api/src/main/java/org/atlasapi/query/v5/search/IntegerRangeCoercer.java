package org.atlasapi.query.v5.search;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

public class IntegerRangeCoercer extends RangeCoercer<Integer> {

    private IntegerRangeCoercer() {
        super(Integer::parseInt);
    }

    public static IntegerRangeCoercer create() {
        return new IntegerRangeCoercer();
    }

    @Override
    protected Integer[] bisectAndCoerce(String value) throws InvalidAttributeValueException {

        String[] bisected = value.split(RANGE_SEPARATOR);
        if (bisected.length != 2) {
            throw new InvalidAttributeValueException(value);
        }

        try {
            Integer[] coerced = new Integer[2];
            for (int i = 0; i < 2; i++) {
                coerced[i] = coercerFunction.apply(bisected[i]);
            }
            return coerced;
        } catch (NumberFormatException e) {
            throw new InvalidAttributeValueException(value);
        }
    }
}
