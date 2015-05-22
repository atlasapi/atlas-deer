package org.atlasapi.entity;

import org.atlasapi.media.entity.Publisher;

public class PlayerRef extends ResourceRef {
    public PlayerRef(Id id, Publisher source) {
        super(id, source);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.PLAYER;
    }
}
