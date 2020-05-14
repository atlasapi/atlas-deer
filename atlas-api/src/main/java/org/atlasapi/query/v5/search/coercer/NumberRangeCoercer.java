package org.atlasapi.query.v5.search.coercer;

import java.util.function.Function;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

public class NumberRangeCoercer<T extends Number> extends RangeCoercer<T> {

    private NumberRangeCoercer(Function<String, T> coercerFunction) {
        super(coercerFunction);
    }

    public static NumberRangeCoercer<Integer> createIntegerCoercer() {
        return new NumberRangeCoercer<>(Integer::parseInt);
    }

    public static NumberRangeCoercer<Long> createLongCoercer() {
        return new NumberRangeCoercer<>(Long::parseLong);
    }

    public static NumberRangeCoercer<Float> createFloatCoercer() {
        return new NumberRangeCoercer<>(Float::parseFloat);
    }

    @Override
    protected Range<T> bisectAndCoerce(String value) throws InvalidAttributeValueException {

        String[] bisected = value.split(RANGE_SEPARATOR);
        if (bisected.length != 2) {
            throw new InvalidAttributeValueException(value);
        }

        try {
            return new Range<>(
                    coercerFunction.apply(bisected[0]),
                    coercerFunction.apply(bisected[1])
            );
        } catch (NumberFormatException e) {
            throw new InvalidAttributeValueException(value);
        }
    }
}
