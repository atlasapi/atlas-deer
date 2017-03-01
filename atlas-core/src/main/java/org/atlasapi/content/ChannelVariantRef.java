package org.atlasapi.content;

import org.atlasapi.entity.Id;

public class ChannelVariantRef {

    private final String title;
    private final Id id;

    private ChannelVariantRef(String title, Id id) {
        this.title = title;
        this.id = id;
    }

    public static ChannelVariantRef create(String title, Id id) {
        return new ChannelVariantRef(title, id);
    }

    public String getTitle() {
        return title;
    }

    public Id getId() {
        return id;
    }

}
