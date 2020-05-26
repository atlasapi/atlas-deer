package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Id;
import org.atlasapi.query.common.coercers.IdCoercer;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class IdAttribute extends SherlockAttribute<Id, String, KeywordMapping> {

    private final NumberToShortStringCodec idCodec;

    public IdAttribute(
            SherlockParameter parameter,
            KeywordMapping mapping,
            NumberToShortStringCodec idCodec
    ) {
        super(parameter, mapping, IdCoercer.create(idCodec));
        this.idCodec = idCodec;
    }

    @Override
    protected SimpleParameter<String> createParameter(KeywordMapping mapping, @Nonnull Id value) {
        return TermParameter.of(mapping, idCodec.encode(value.toBigInteger()));
    }
}
