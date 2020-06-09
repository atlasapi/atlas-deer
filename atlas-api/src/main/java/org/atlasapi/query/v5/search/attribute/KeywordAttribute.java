package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.BooleanCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class KeywordAttribute extends TermAttribute<String, KeywordMapping> {

    public KeywordAttribute(
            SherlockParameter parameter,
            KeywordMapping mapping
    ) {
        super(parameter, mapping, StringCoercer.create());
    }

    @Override
    protected SimpleParameter<String> createParameter(KeywordMapping mapping, @Nonnull String value) {
        return TermParameter.of(mapping, value);
    }
}
