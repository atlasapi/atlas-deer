package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.coercers.IdCoercer;

import javax.annotation.Nonnull;

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
