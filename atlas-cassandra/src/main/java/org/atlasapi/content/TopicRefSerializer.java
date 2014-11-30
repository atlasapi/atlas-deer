package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.TopicProtos;
import org.atlasapi.serialization.protobuf.TopicProtos.Topic.Builder;

public class TopicRefSerializer {

    public ContentProtos.TopicRef serialize(TopicRef topicRef) {
        ContentProtos.TopicRef.Builder ref = ContentProtos.TopicRef.newBuilder();
        ref.setTopic(serialize(topicRef.getTopic()));
        if (topicRef.getOffset() != null) {
            ref.setOffset(topicRef.getOffset());
        }
        if (topicRef.getRelationship() != null) {
            ref.setRelationship(topicRef.getRelationship().toString());
        }
        if (topicRef.isSupervised() != null) {
            ref.setSupervised(topicRef.isSupervised());
        }
        if (topicRef.getWeighting() != null) {
            ref.setWeighting(topicRef.getWeighting());
        }
        return ref.build();
    }

    private Builder serialize(Id topic) {
        return TopicProtos.Topic.newBuilder().setId(topic.longValue());
    }
    
    public TopicRef deserialize(ContentProtos.TopicRef ref) {
        return new TopicRef(
            Id.valueOf(ref.getTopic().getId()),
            ref.hasWeighting() ? ref.getWeighting() : null,
            ref.hasSupervised() ? ref.getSupervised() : null,
            ref.hasRelationship() ? TopicRef.Relationship.fromString(ref.getRelationship()).get() : null,
            ref.hasOffset() ? ref.getOffset() : null);
    }
}
