package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.EnumCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class EnumAttribute<T extends Enum<T>> extends SherlockAttributeSingle<T, String, KeywordMapping> {

    public EnumAttribute(SherlockParameter parameter, KeywordMapping mapping, EnumCoercer<T> coercer) {
        super(parameter, mapping, coercer);
    }

    @Override
    protected SimpleParameter<String> createParameter(KeywordMapping mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value.toString());
    }
}
