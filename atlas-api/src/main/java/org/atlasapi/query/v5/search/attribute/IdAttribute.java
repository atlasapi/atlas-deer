package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Id;
import org.atlasapi.query.common.coercers.IdCoercer;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class IdAttribute extends SherlockSingleMappingAttribute<Id, Long, KeywordMapping<Long>> {

    public IdAttribute(
            SherlockParameter parameter,
            KeywordMapping<Long> mapping,
            IdCoercer coercer
    ) {
        super(parameter, coercer, mapping);
    }

    @Override
    protected SingleValueParameter<Long> createParameter(KeywordMapping<Long> mapping, @Nonnull Id value) {
        return TermParameter.of(mapping, value.longValue());
    }
}
