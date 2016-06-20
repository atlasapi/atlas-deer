package org.atlasapi.topic;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.DescribedSerializer;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.TopicProtos;
import org.atlasapi.source.Sources;
import org.atlasapi.topic.Topic.Type;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

public class TopicSerializer implements Serializer<Topic, byte[]> {

    private final DescribedSerializer<Topic> describedSerializer = new DescribedSerializer<>();

    @Override
    public byte[] serialize(Topic topic) {
        return serializeToBuilder(topic).build().toByteArray();
    }

    @Override
    public Topic deserialize(byte[] bytes) {
        try {
            TopicProtos.Topic msg = TopicProtos.Topic.parseFrom(bytes);
            return deserialize(msg);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }

    public TopicProtos.Topic.Builder serializeToBuilder(Topic src) {
        TopicProtos.Topic.Builder builder = TopicProtos.Topic.newBuilder();

        builder.setDescribed(describedSerializer.serialize(src));

        if (src.getSource() != null) {
            builder.setSource(src.getSource().key());
        }
        if (src.getType() != null) {
            builder.setType(src.getType().key());
        }
        if (src.getNamespace() != null) {
            builder.setNamespace(src.getNamespace());
        }
        if (src.getValue() != null) {
            builder.setValue(src.getValue());
        }

        return builder;
    }

    public Topic deserialize(TopicProtos.Topic msg) {
        Topic topic = new Topic();

        if (msg.hasDescribed()) {
            describedSerializer.deserialize(msg.getDescribed(), topic);
        }

        if (msg.hasType()) {
            topic.setType(Type.fromKey(msg.getType()));
        }
        if (msg.hasNamespace()) {
            topic.setNamespace(msg.getNamespace());
        }
        if (msg.hasValue()) {
            topic.setValue(msg.getValue());
        }
        if (msg.hasSource()) {
            topic.setPublisher(Sources.fromPossibleKey(msg.getSource()).get());
        }

        deserializeDeprecatedFields(msg, topic);

        return topic;
    }

    // This method must check the existing value in each field before modifying it so that
    // if a field is serialized in two places the non deprecated version will take precedence
    private void deserializeDeprecatedFields(TopicProtos.Topic msg, Topic topic) {
        if (msg.hasId() && topic.getId() == null) {
            topic.setId(msg.getId());
        }
        if (topic.getAliases().isEmpty()) {
            ImmutableList<Alias> aliases = msg.getAliasesList().stream()
                    .map(alias -> new Alias(alias.getNamespace(), alias.getValue()))
                    .collect(MoreCollectors.toImmutableList());
            topic.setAliases(aliases);
        }
        if (msg.getTitleCount() > 0 && topic.getTitle() == null) {
            topic.setTitle(msg.getTitle(0).getValue());
        }
        if (msg.getDescriptionCount() > 0 && topic.getDescription() == null) {
            topic.setDescription(msg.getDescription(0).getValue());
        }
        if (msg.getImageCount() > 0 && topic.getImage() == null) {
            topic.setImage(msg.getImage(0));
        }
        if (msg.getThumbnailCount() > 0 && topic.getThumbnail() == null) {
            topic.setThumbnail(msg.getThumbnail(0));
        }
        if (topic.getMediaType() == null) {
            topic.setMediaType(null);
        }
    }
}
