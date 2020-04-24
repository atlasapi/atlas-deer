package org.atlasapi.query.v5.search;

import java.util.List;
import java.util.function.Function;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;
import org.atlasapi.query.v5.search.RangeOrTerm;

import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

import com.google.common.collect.ImmutableList;
import joptsimple.internal.Strings;

public abstract class RangeCoercer<T> {

    protected final String RANGE_SEPARATOR = "-";
    protected final Function<String, T> coercerFunction;

    protected RangeCoercer(Function<String, T> coercerFunction) {
        this.coercerFunction = coercerFunction;
    }

    private RangeOrTerm<?> splitAndCoerce(ChildTypeMapping<T> mapping, String value) throws InvalidAttributeValueException {
        if (Strings.isNullOrEmpty(value)) {
            throw new InvalidAttributeValueException(value);
        } else if (value.startsWith(RANGE_SEPARATOR)) {
            return RangeOrTerm.of(RangeParameter.to(mapping, coercerFunction.apply(value.substring(1))));
        } else if (value.endsWith(RANGE_SEPARATOR)) {
            return RangeOrTerm.of(RangeParameter.from(mapping, coercerFunction.apply(value.substring(0, value.length()-1))));
        } else {
            try {
                T parsedValue = coercerFunction.apply(value);
                return RangeOrTerm.of(TermParameter.of(mapping, parsedValue));
            } catch (Exception e) {
                T[] parseValues = bisectAndCoerce(value);
                return RangeOrTerm.of(RangeParameter.of(mapping, parseValues[0], parseValues[1]));
            }
        }
    }

    protected abstract T[] bisectAndCoerce(String value) throws InvalidAttributeValueException;

    public List<RangeOrTerm<?>> apply(ChildTypeMapping<T> mapping, Iterable<String> values) throws InvalidAttributeValueException {
        ImmutableList.Builder<RangeOrTerm<?>> ranges = ImmutableList.builder();

        for (String value : values) {
            RangeOrTerm<?> range = splitAndCoerce(mapping, value);
            ranges.add(range);
        }

        return ranges.build();
    }
}
