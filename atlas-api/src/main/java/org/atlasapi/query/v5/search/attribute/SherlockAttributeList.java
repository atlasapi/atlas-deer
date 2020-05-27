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

public abstract class SherlockAttributeList<FROM, TO, M extends ChildTypeMapping<TO>>
        extends SherlockAttribute<FROM, TO, M> {

    public SherlockAttributeList(
            SherlockParameter parameter,
            M mapping,
            AttributeCoercer<FROM> coercer
    ) {
        super(parameter, mapping, coercer);
    }

    @Override
    public List<SimpleParameter<TO>> coerce(List<String> values) throws InvalidAttributeValueException {
        return createParameters(mapping, coercer.apply(values));
    }

    protected abstract List<SimpleParameter<TO>> createParameters(M mapping, @Nonnull List<FROM> value);
}
