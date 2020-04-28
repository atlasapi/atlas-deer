package org.atlasapi.query.v5.search;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.FilterParameter;
import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class TermAttribute<T> extends SherlockAttribute<T, ChildTypeMapping<T>> {

    private final AttributeCoercer<T> coercer;

    public TermAttribute(
            String parameterName,
            ChildTypeMapping<T> mapping,
            AttributeCoercer<T> coercer
    ) {
        super(parameterName, mapping);
        this.coercer = coercer;
    }

    @Override
    public List<NamedParameter<T>> coerce(List<String> value) throws InvalidAttributeValueException {
        return coercer.apply(value).stream()
                .map(v -> TermParameter.of(getMapping(), v))
                .collect(Collectors.toList());
    }
}
