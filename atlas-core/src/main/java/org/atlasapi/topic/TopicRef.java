package org.atlasapi.topic;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

public class TopicRef extends ResourceRef {

    public TopicRef(Id id, Publisher source) {
        super(id, source);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.TOPIC;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof TopicRef) {
            TopicRef other = (TopicRef) that;
            return id.equals(other.id)
                    && source.equals(other.source);
        }
        return false;
    }

}
