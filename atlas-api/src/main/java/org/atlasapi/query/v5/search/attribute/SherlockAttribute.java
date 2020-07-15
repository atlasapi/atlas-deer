package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.Parameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockAttribute<FROM, P extends Parameter, TO, M extends ChildTypeMapping<TO>> {

    protected final SherlockParameter parameter;
    protected final AttributeCoercer<FROM> coercer;
    protected final M[] mappings;

    @SafeVarargs
    public SherlockAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            M... mappings
    ) {
        if (parameter.getType() == SherlockParameter.Type.SEARCH) {
            assert this.getClass().isAssignableFrom(SearchAttribute.class);
        } else {
            assert !this.getClass().isAssignableFrom(SearchAttribute.class);
        }
        this.parameter = parameter;
        this.coercer = coercer;
        this.mappings = mappings;
    }

    public SherlockParameter getParameter() {
        return parameter;
    }

    public List<P> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .filter(Objects::nonNull)
                .map(v -> createParameter(mappings, v))
                .collect(Collectors.toList());
    }

    protected abstract P createParameter(M[] mappings, FROM value);
}
