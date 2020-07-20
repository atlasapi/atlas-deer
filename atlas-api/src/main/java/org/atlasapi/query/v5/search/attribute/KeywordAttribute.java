package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.coercers.BooleanCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;

import com.metabroadcast.sherlock.client.search.parameter. SingleValueParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class KeywordAttribute<T> extends TermAttribute<T, KeywordMapping<T>> {

    public KeywordAttribute(
            SherlockParameter parameter,
            KeywordMapping<T> mapping,
            AttributeCoercer<T> coercer
    ) {
        super(parameter, mapping, coercer);
    }

    @Override
    protected  SingleValueParameter<T> createParameter(KeywordMapping<T> mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value);
    }
}
