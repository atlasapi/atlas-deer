package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.EnumCoercer;

import com.metabroadcast.sherlock.client.search.parameter. SingleValueParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class EnumAttribute<T extends Enum<T>> extends
        SherlockSingleMappingAttribute<T, String, KeywordMapping> {

    public EnumAttribute(SherlockParameter parameter, KeywordMapping mapping, EnumCoercer<T> coercer) {
        super(parameter, coercer, mapping);
    }

    @Override
    protected  SingleValueParameter<String> createParameter(KeywordMapping mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value.toString());
    }
}
