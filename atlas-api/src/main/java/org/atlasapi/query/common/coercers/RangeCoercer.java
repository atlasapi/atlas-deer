package org.atlasapi.query.common.coercers;

import java.util.List;
import java.util.function.Function;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import sherlock_client_shaded.com.metabroadcast.sherlock.client.search.Range;

import com.google.common.collect.ImmutableList;
import joptsimple.internal.Strings;

public abstract class RangeCoercer<T> implements AttributeCoercer<Range<T>> {

    protected final String RANGE_SEPARATOR = "-";
    protected final Function<String, T> coercerFunction;

    protected RangeCoercer(Function<String, T> coercerFunction) {
        this.coercerFunction = coercerFunction;
    }

    private Range<T> splitAndCoerce(String value) throws InvalidAttributeValueException {
        if (Strings.isNullOrEmpty(value)) {
            throw new InvalidAttributeValueException(value);
        } else if (value.startsWith(RANGE_SEPARATOR)) {
            return Range.to(coercerFunction.apply(value.substring(1)));
        } else if (value.endsWith(RANGE_SEPARATOR)) {
            return Range.from(coercerFunction.apply(value.substring(0, value.length()-1)));
        } else {
            try {
                T parsedValue = coercerFunction.apply(value);
                return Range.of(parsedValue);
            } catch (Exception e) {
                T[] parseValues = bisectAndCoerce(value);
                return new Range<>(parseValues[0], parseValues[1]);
            }
        }
    }

    protected abstract T[] bisectAndCoerce(String value) throws InvalidAttributeValueException;

    @Override
    public List<Range<T>> apply(Iterable<String> values) throws InvalidAttributeValueException {
        ImmutableList.Builder<Range<T>> ranges = ImmutableList.builder();

        for (String value : values) {
            Range<T> range = splitAndCoerce(value);
            ranges.add(range);
        }

        return ranges.build();
    }
}
