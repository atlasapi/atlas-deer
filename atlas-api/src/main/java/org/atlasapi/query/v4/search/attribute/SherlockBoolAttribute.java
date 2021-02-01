package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import org.atlasapi.query.common.coercers.AttributeCoercer;

public abstract class SherlockBoolAttribute<FROM, TO, M extends ChildTypeMapping<TO>> extends
        SherlockAttribute<FROM, BoolParameter, TO, M> {

    @SafeVarargs
    public SherlockBoolAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            M... mappings
    ) {
        super(parameter, coercer, mappings);
    }

    protected abstract BoolParameter createParameter(M[] mappings, FROM value);
}
