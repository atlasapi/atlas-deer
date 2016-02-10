package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;

public class BlackoutRestrictionSerializer {

    public ContentProtos.BlackoutRestriction.Builder serialize(
            BlackoutRestriction blackoutRestriction) {
        ContentProtos.BlackoutRestriction.Builder builder = ContentProtos.BlackoutRestriction.newBuilder();
        if (blackoutRestriction.getAll() != null) {
            builder.setAll(blackoutRestriction.getAll());
        }
        return builder;
    }

    public BlackoutRestriction deserialize(ContentProtos.BlackoutRestriction msg) {
        return msg.hasAll() ? new BlackoutRestriction(msg.getAll()) : new BlackoutRestriction(null);
    }
}
