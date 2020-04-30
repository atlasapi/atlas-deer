package org.atlasapi.query.v5.search.attribute;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Id;
import org.atlasapi.query.common.coercers.IdCoercer;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class IdAttribute extends SherlockAttribute<Id, String, KeywordMapping> {

    private final NumberToShortStringCodec idCodec;

    public IdAttribute(
            String parameterName,
            KeywordMapping mapping,
            NumberToShortStringCodec idCodec
    ) {
        super(parameterName, mapping, IdCoercer.create(idCodec));
        this.idCodec = idCodec;
    }

    @Override
    protected NamedParameter<String> createParameter(KeywordMapping mapping, @Nonnull Id value) {
        return TermParameter.of(mapping, idCodec.encode(value.toBigInteger()));
    }
}
