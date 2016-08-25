package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Tag;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

public class TagSerialization {

    public Tag serialize(org.atlasapi.content.Tag tag) {
        if (tag == null) {
            return null;
        }
        Tag internal = new Tag();

        Id topic = tag.getTopic();
        if (topic != null) {
            internal.setTopic(topic.longValue());
        }

        Publisher publisher = tag.getPublisher();
        if (publisher != null) {
            internal.setPublisher(publisher.key());
        }

        internal.setSupervised(tag.isSupervised());
        internal.setWeighting(tag.getWeighting());

        org.atlasapi.content.Tag.Relationship relationship = tag.getRelationship();
        if (relationship != null) {
            internal.setRelationship(relationship.name());
        }

        internal.setOffset(tag.getOffset());

        return internal;
    }

    public org.atlasapi.content.Tag deserialize(Tag internal) {
        if (internal == null) {
            return null;
        }

        org.atlasapi.content.Tag tag = new org.atlasapi.content.Tag(
                internal.getTopic(),
                internal.getWeighting(),
                internal.getSupervised(),
                org.atlasapi.content.Tag.Relationship.valueOf(internal.getRelationship())
        );
        if (internal.getOffset() != null) {
            tag.setOffset(internal.getOffset());
        }
        if (internal.getPublisher() != null) {
            tag.setPublisher(Publisher.fromKey(internal.getPublisher()).requireValue());
        }

        return tag;
    }

}