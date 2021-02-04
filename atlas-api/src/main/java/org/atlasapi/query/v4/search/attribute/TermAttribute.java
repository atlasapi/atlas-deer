package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import org.atlasapi.query.common.coercers.AttributeCoercer;

import javax.annotation.Nonnull;

public abstract class TermAttribute<T, M extends ChildTypeMapping<T>> extends
        SherlockSingleMappingAttribute<T, T, M> {

    public TermAttribute(
            SherlockParameter parameter,
            M mapping,
            AttributeCoercer<T> coercer
    ) {
        super(parameter, coercer, mapping);
    }

    @Override
    protected SingleValueParameter<T> createParameter(M mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value);
    }
}
