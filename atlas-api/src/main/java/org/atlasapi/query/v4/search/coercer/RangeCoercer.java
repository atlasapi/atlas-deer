package org.atlasapi.query.v4.search.coercer;

import com.google.common.collect.ImmutableList;
import joptsimple.internal.Strings;
import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public abstract class RangeCoercer<T> implements AttributeCoercer<Range<T>> {

    protected static final char RANGE_SEPARATOR_CHAR = '-';
    protected static final String RANGE_SEPARATOR = String.valueOf(RANGE_SEPARATOR_CHAR);
    protected final Function<String, T> coercerFunction;

    protected RangeCoercer(Function<String, T> coercerFunction) {
        this.coercerFunction = coercerFunction;
    }

    private Range<T> splitAndCoerce(String value) throws InvalidAttributeValueException {
        if (Strings.isNullOrEmpty(value)) {
            throw new InvalidAttributeValueException(value);
        } else if (value.startsWith(RANGE_SEPARATOR)) {
            return new Range<>(null, coercerFunction.apply(value.substring(1)));
        } else if (value.endsWith(RANGE_SEPARATOR)) {
            return new Range<>(coercerFunction.apply(value.substring(0, value.length()-1)), null);
        } else {
            Optional<T> parsedValue = maybeCoerce(value);
            if (parsedValue.isPresent()) {
                return new Range<>(parsedValue.get(), parsedValue.get());
            } else {
                return bisectAndCoerce(value);
            }
        }
    }

    private Optional<T> maybeCoerce(String value) {
        try {
            T parsedValue = coercerFunction.apply(value);
            return Optional.of(parsedValue);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    protected abstract Range<T> bisectAndCoerce(String value) throws InvalidAttributeValueException;

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
