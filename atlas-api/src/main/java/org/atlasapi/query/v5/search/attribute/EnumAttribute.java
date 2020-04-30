package org.atlasapi.query.v5.search.attribute;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.EnumCoercer;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class EnumAttribute<T extends Enum<T>> extends SherlockAttribute<T, String, KeywordMapping> {

    public EnumAttribute(String parameterName, KeywordMapping mapping, EnumCoercer<T> coercer) {
        super(parameterName, mapping, coercer);
    }

    @Override
    protected NamedParameter<String> createParameter(KeywordMapping mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value.toString());
    }
}
