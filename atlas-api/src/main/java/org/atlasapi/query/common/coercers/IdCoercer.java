package org.atlasapi.query.common.coercers;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class IdCoercer extends AbstractAttributeCoercer<Id> {

    private final NumberToShortStringCodec idCodec;

    private IdCoercer(NumberToShortStringCodec idCodec) {
        this.idCodec = checkNotNull(idCodec);
    }

    public static IdCoercer create(NumberToShortStringCodec idCodec) {
        return new IdCoercer(idCodec);
    }

    @Override
    protected Id coerce(String input) {
        return Id.valueOf(idCodec.decode(input));
    }
}
