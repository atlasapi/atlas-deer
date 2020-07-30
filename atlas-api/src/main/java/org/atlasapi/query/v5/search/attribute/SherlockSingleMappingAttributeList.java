package org.atlasapi.query.v5.search.attribute;

import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockSingleMappingAttributeList<FROM, TO, M extends ChildTypeMapping<TO>>
        extends SherlockSingleMappingAttribute<FROM, TO, M> {

    public SherlockSingleMappingAttributeList(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            M mapping
    ) {
        super(parameter, coercer, mapping);
    }

    @Override
    public List<SingleValueParameter<TO>> coerce(List<String> values) throws InvalidAttributeValueException {
        return createParameters(getMapping(), coercer.apply(values));
    }

    protected abstract List<SingleValueParameter<TO>> createParameters(M mapping, @Nonnull List<FROM> value);

    @Override
    protected SingleValueParameter<TO> createParameter(M[] mappings, FROM value) {
        return null;
    }

    @Override
    protected SingleValueParameter<TO> createParameter(M mapping, FROM value) {
        return null;
    }
}
