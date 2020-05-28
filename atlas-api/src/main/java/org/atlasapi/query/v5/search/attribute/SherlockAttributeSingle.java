package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

import com.google.common.collect.ImmutableList;

public abstract class SherlockAttributeSingle<FROM, TO, M extends ChildTypeMapping<TO>>
        extends SherlockAttribute<FROM, TO, M> {

    public SherlockAttributeSingle(
            SherlockParameter parameter,
            M mapping,
            AttributeCoercer<FROM> coercer
    ) {
        super(parameter, mapping, coercer);
    }

    @Override
    public List<SimpleParameter<TO>> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .filter(Objects::nonNull)
                .map(v -> createParameter(mapping, v))
                .collect(Collectors.toList());
    }

    protected abstract SimpleParameter<TO> createParameter(M mapping, @Nonnull FROM value);
}
