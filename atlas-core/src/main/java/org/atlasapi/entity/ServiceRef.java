package org.atlasapi.entity;

import org.atlasapi.media.entity.Publisher;

public class ServiceRef extends ResourceRef {
    public ServiceRef(Id id, Publisher source) {
        super(id, source);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.SERVICE;
    }
}
