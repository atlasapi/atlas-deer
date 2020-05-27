package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockAttribute<FROM, TO, M extends ChildTypeMapping<TO>> {

    protected final SherlockParameter parameter;
    protected final M mapping;
    protected final AttributeCoercer<FROM> coercer;

    public SherlockAttribute(
            SherlockParameter parameter,
            M mapping,
            AttributeCoercer<FROM> coercer
    ) {
        this.parameter = parameter;
        this.mapping = mapping;
        this.coercer = coercer;
    }

    public SherlockParameter getParameter() {
        return parameter;
    }

    public M getMapping() {
        return mapping;
    }

    public abstract List<SimpleParameter<TO>> coerce(List<String> values)
            throws InvalidAttributeValueException;
}
