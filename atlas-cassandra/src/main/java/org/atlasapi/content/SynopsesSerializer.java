package org.atlasapi.content;

import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class SynopsesSerializer implements Serializer<Synopses, CommonProtos.Synopses> {

    @Override
    public CommonProtos.Synopses serialize(Synopses source) {
        CommonProtos.Synopses.Builder builder = CommonProtos.Synopses.newBuilder();

        if(source.getShortDescription() != null) {
            builder.setShortDescription(builder.getShortDescriptionBuilder()
                    .setValue(source.getShortDescription()));
        }
        if(source.getMediumDescription() != null) {
            builder.setMediumDescription(builder.getMediumDescriptionBuilder()
                    .setValue(source.getMediumDescription()));
        }
        if(source.getLongDescription() != null) {
            builder.setLongDescription(builder.getLongDescriptionBuilder()
                    .setValue(source.getLongDescription()));
        }

        return builder.build();
    }

    @Override
    public Synopses deserialize(CommonProtos.Synopses serialized) {
        Synopses synopses = new Synopses();

        if(serialized.hasShortDescription()) {
            synopses.setShortDescription(serialized.getShortDescription().getValue());
        }
        if(serialized.hasMediumDescription()) {
            synopses.setMediumDescription(serialized.getMediumDescription().getValue());
        }
        if(serialized.hasLongDescription()) {
            synopses.setLongDescription(serialized.getLongDescription().getValue());
        }

        return synopses;
    }

}
