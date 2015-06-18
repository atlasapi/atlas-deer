package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;

public final class RestrictionSerializer {

    public ContentProtos.Restriction.Builder serialize(Restriction restriction) {
        ContentProtos.Restriction.Builder message = ContentProtos.Restriction.newBuilder();
        if (restriction.getMinimumAge() != null) {
            message.setMinimumAge(restriction.getMinimumAge());
        }
        if (restriction.isRestricted() != null) {
            message.setRestricted(restriction.isRestricted());
        }
        if (restriction.getMessage() != null) {
            message.setRestrictionMessage(restriction.getMessage());
        }
        if (restriction.getAuthority() != null) {
            message.setAuthority(restriction.getAuthority());
        }
        if (restriction.getRating() != null) {
            message.setRating(restriction.getRating());
        }
        return message;
    }
    
    public Restriction deserialize(ContentProtos.Restriction msg) {
        Restriction restriction = new Restriction();
        restriction.setMinimumAge(msg.hasMinimumAge() ? msg.getMinimumAge() : null);
        restriction.setRestricted(msg.hasRestricted() ? msg.getRestricted() : null);
        restriction.setMessage(msg.hasRestrictionMessage() ? msg.getRestrictionMessage() : null);
        restriction.setAuthority(msg.hasAuthority() ? msg.getAuthority() : null);
        restriction.setRating(msg.hasRating() ? msg.getRating() : null);
        return restriction;
    }
    
}
