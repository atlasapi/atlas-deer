package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;
import org.atlasapi.query.common.coercers.EnumCoercer;

import javax.annotation.Nonnull;

public class EnumAttribute<T extends Enum<T>> extends
        SherlockSingleMappingAttribute<T, String, KeywordMapping<String>> {

    public EnumAttribute(SherlockParameter parameter, KeywordMapping<String> mapping, EnumCoercer<T> coercer) {
        super(parameter, coercer, mapping);
    }

    @Override
    protected SingleValueParameter<String> createParameter(KeywordMapping<String> mapping, @Nonnull T value) {
        return TermParameter.of(mapping, value.toString());
    }
}
