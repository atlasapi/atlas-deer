package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class TermAttribute<T, M extends ChildTypeMapping<T>> extends SherlockAttributeSingle<T, T, M> {

    public TermAttribute(
            SherlockParameter parameter,
            M mapping,
            AttributeCoercer<T> coercer
    ) {
        super(parameter, mapping, coercer);
    }

    @Override
    protected SimpleParameter<T> createParameter(M mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value);
    }
}
