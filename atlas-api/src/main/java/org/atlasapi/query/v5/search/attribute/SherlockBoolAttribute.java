package org.atlasapi.query.v5.search.attribute;

import org.atlasapi.query.common.coercers.AttributeCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockBoolAttribute<FROM, TO, M extends ChildTypeMapping<TO>> extends
        SherlockAttribute<FROM, SingleClauseBoolParameter, TO, M> {

    @SafeVarargs
    public SherlockBoolAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            M... mappings
    ) {
        super(parameter, coercer, mappings);
    }

    protected abstract SingleClauseBoolParameter createParameter(M[] mappings, FROM value);
}
