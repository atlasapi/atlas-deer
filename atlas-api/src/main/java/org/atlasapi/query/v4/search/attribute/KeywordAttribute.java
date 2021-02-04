package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;
import org.atlasapi.query.common.coercers.AttributeCoercer;

import javax.annotation.Nonnull;

public class KeywordAttribute<T> extends TermAttribute<T, KeywordMapping<T>> {

    public KeywordAttribute(
            SherlockParameter parameter,
            KeywordMapping<T> mapping,
            AttributeCoercer<T> coercer
    ) {
        super(parameter, mapping, coercer);
    }

    @Override
    protected SingleValueParameter<T> createParameter(KeywordMapping<T> mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value);
    }
}
