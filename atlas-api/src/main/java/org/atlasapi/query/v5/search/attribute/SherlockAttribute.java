package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockAttribute<FROM, TO, M extends ChildTypeMapping<TO>> {

    private final String parameterName;
    private final M mapping;
    private final AttributeCoercer<FROM> coercer;

    public SherlockAttribute(
            String parameterName,
            M mapping,
            AttributeCoercer<FROM> coercer
    ) {
        this.parameterName = parameterName;
        this.mapping = mapping;
        this.coercer = coercer;
    }

    public String getParameterName() {
        return parameterName;
    }

    public M getMapping() {
        return mapping;
    }

    public List<NamedParameter<TO>> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .map(v -> createParameter(mapping, v))
                .collect(Collectors.toList());
    }

    protected abstract NamedParameter<TO> createParameter(M mapping, FROM value);
}
