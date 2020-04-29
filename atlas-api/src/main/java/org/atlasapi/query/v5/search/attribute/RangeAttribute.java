package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.FilterParameter;
import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;

public class RangeAttribute<T> extends SherlockAttribute<T, RangeTypeMapping<T>> {

    private final RangeCoercer<T> coercer;

    public RangeAttribute(
            String parameterName,
            RangeTypeMapping<T> mapping,
            RangeCoercer<T> coercer
    ) {
        super(parameterName, mapping);
        this.coercer = coercer;
    }

    @Override
    public List<NamedParameter<T>> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .map(v -> coerceToRangeOrTerm(getMapping(), v))
                .collect(Collectors.toList());
    }

    private FilterParameter<T> coerceToRangeOrTerm(
            RangeTypeMapping<T> mapping,
            Range<T> value
    ) {
        if (value.getFrom() == value.getTo()) {
            return TermParameter.of(mapping, value.getFrom());
        } else {
            return RangeParameter.of(mapping, value.getFrom(), value.getTo());
        }
    }
    
}
