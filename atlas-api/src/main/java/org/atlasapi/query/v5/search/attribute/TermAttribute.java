package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class TermAttribute<T> extends SherlockAttribute<T, T, ChildTypeMapping<T>> {

    public TermAttribute(
            String parameterName,
            ChildTypeMapping<T> mapping,
            AttributeCoercer<T> coercer
    ) {
        super(parameterName, mapping, coercer);
    }

    @Override
    protected NamedParameter<T> createParameter(ChildTypeMapping<T> mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value);
    }
}
