package org.atlasapi.application.sources;

import java.math.BigInteger;
import java.util.Optional;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class SourceIdCodec {

    private static final int ID_MAGNIFIER = 1000;
    private final NumberToShortStringCodec idCodec;

    private SourceIdCodec(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }

    public static SourceIdCodec create() {
        return new SourceIdCodec(new SubstitutionTableNumberCodec());
    }

    public static SourceIdCodec createWithCodec(NumberToShortStringCodec idCodec) {
        return new SourceIdCodec(idCodec);
    }

    public String encode(Publisher source) {
        return idCodec.encode(BigInteger.valueOf(ID_MAGNIFIER + source.ordinal()));
    }

    public Optional<Publisher> decode(String id) {
        try {
            return Optional.ofNullable(Publisher.values()[idCodec.decode(id).intValue()
                    - ID_MAGNIFIER]);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<Publisher> decode(Id id) {
        try {
            return Optional.ofNullable(Publisher.values()[id.toBigInteger().intValue()
                    - ID_MAGNIFIER]);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}