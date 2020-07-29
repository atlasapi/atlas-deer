package org.atlasapi.query.v5.search.attribute;

import org.atlasapi.query.common.coercers.AttributeCoercer;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

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
