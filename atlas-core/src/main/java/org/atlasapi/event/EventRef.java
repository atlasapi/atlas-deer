package org.atlasapi.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

public class EventRef extends ResourceRef {

    public EventRef(Id id, Publisher source) {
        super(checkNotNull(id), checkNotNull(source));
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.EVENT;
    }
}
