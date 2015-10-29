package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;


public class EventRef extends ResourceRef {

    public EventRef(Id id, Publisher source) {
        super(id, source);
    }

    @FieldName("resource_type")
    @Override
    public ResourceType getResourceType() {
        return ResourceType.EVENT;
    }
}
