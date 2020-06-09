package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockBoolAttribute<FROM, TO, M extends ChildTypeMapping<TO>> extends
        SherlockAttribute<FROM, BoolParameter, TO, M> {

    protected final M[] mappings;

    @SafeVarargs
    public SherlockBoolAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            M... mappings
    ) {
        super(parameter, coercer);
        this.mappings = mappings;
    }

    @Override
    public List<BoolParameter> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .filter(Objects::nonNull)
                .map(v -> createParameter(mappings, v))
                .collect(Collectors.toList());
    }

    protected abstract BoolParameter createParameter(M[] mappings, FROM value);
}
