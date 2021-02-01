package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import org.atlasapi.query.common.coercers.AttributeCoercer;

public abstract class SherlockSingleMappingAttribute<FROM, TO, M extends ChildTypeMapping<TO>>
        extends SherlockAttribute<FROM, SingleValueParameter<TO>, TO, M> {

    public SherlockSingleMappingAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            M mapping
    ) {
        super(parameter, coercer, mapping);
    }

    protected M getMapping() {
        return mappings[0];
    }

    public ExistParameter<TO> getExistsParameter(boolean exists) {
        if (exists) {
            return ExistParameter.exists(getMapping());
        } else {
            return ExistParameter.notExists(getMapping());
        }
    }

    @Override
    protected SingleValueParameter<TO> createParameter(M[] mappings, FROM value) {
        return createParameter(mappings[0], value);
    }

    protected abstract SingleValueParameter<TO> createParameter(M mapping, FROM value);
}
