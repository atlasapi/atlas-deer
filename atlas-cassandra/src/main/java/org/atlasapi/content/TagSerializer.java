package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.TopicProtos;
import org.atlasapi.serialization.protobuf.TopicProtos.Topic.Builder;

public class TagSerializer {

    public ContentProtos.TopicRef serialize(Tag tag) {
        ContentProtos.TopicRef.Builder ref = ContentProtos.TopicRef.newBuilder();
        ref.setTopic(serialize(tag.getTopic()));
        if (tag.getOffset() != null) {
            ref.setOffset(tag.getOffset());
        }
        if (tag.getRelationship() != null) {
            ref.setRelationship(tag.getRelationship().toString());
        }
        if (tag.isSupervised() != null) {
            ref.setSupervised(tag.isSupervised());
        }
        if (tag.getWeighting() != null) {
            ref.setWeighting(tag.getWeighting());
        }
        return ref.build();
    }

    private Builder serialize(Id topic) {
        return TopicProtos.Topic.newBuilder().setId(topic.longValue());
    }

    public Tag deserialize(ContentProtos.TopicRef ref) {
        return new Tag(
                Id.valueOf(ref.getTopic().getId()),
                ref.hasWeighting() ? ref.getWeighting() : null,
                ref.hasSupervised() ? ref.getSupervised() : null,
                ref.hasRelationship()
                ? Tag.Relationship.fromString(ref.getRelationship()).get()
                : null,
                ref.hasOffset() ? ref.getOffset() : null
        );
    }
}
