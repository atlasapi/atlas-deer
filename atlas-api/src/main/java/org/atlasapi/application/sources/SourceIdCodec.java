package org.atlasapi.application.sources;

import java.math.BigInteger;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.base.Optional;

public class SourceIdCodec {

    private static final int ID_MAGNIFIER = 1000;
    private final NumberToShortStringCodec idCodec;

    public SourceIdCodec() {
        this(new SubstitutionTableNumberCodec());
    }

    public SourceIdCodec(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }

    public String encode(Publisher source) {
        return idCodec.encode(BigInteger.valueOf(ID_MAGNIFIER + source.ordinal()));
    }

    public Optional<Publisher> decode(String id) {
        try {
            return Optional.fromNullable(Publisher.values()[idCodec.decode(id).intValue()
                    - ID_MAGNIFIER]);
        } catch (Exception e) {
            return Optional.absent();
        }
    }

    public Optional<Publisher> decode(Id id) {
        try {
            return Optional.fromNullable(Publisher.values()[id.toBigInteger().intValue()
                    - ID_MAGNIFIER]);
        } catch (Exception e) {
            return Optional.absent();
        }
    }

}
