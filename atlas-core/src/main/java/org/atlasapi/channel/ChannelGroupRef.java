package org.atlasapi.channel;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

public class ChannelGroupRef extends ResourceRef {

    public ChannelGroupRef(Id id, Publisher source) {
        super(id, source);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.CHANNEL_GROUP;
    }
}
