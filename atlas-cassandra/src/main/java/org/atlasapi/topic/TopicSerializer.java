package org.atlasapi.topic;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.TopicProtos;
import org.atlasapi.serialization.protobuf.TopicProtos.Topic.Builder;
import org.atlasapi.source.Sources;
import org.atlasapi.topic.Topic.Type;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

public class TopicSerializer implements Serializer<Topic, byte[]> {

    @Override
    public byte[] serialize(Topic topic) {
        Builder msg = TopicProtos.Topic.newBuilder()
            .setId(topic.getId().longValue())
            .setSource(topic.getSource().key());
        if (topic.getType() != null) {
            msg.setType(topic.getType().key());
        }
        for (Alias alias : topic.getAliases()) {
            msg.addAliasesBuilder()
                .setNamespace(alias.getNamespace())
                .setValue(alias.getValue());
        }
        if (topic.getTitle() != null) {
            msg.addTitleBuilder().setValue(topic.getTitle());
        }
        if (topic.getDescription() != null) {
            msg.addDescriptionBuilder().setValue(topic.getDescription());
        }
        if (topic.getImage() != null) {
            msg.addImage(topic.getImage());
        }
        if (topic.getThumbnail() != null) {
            msg.addThumbnail(topic.getThumbnail());
        }
        return msg.build().toByteArray();
    }

    @Override
    public Topic deserialize(byte[] bytes) {
        try {
            TopicProtos.Topic msg;
            msg = TopicProtos.Topic.parseFrom(bytes);
            Topic topic = new Topic(msg.getId());
            topic.setPublisher(Sources.fromPossibleKey(msg.getSource()).get());
            if (msg.hasType()) {
                topic.setType(Type.fromKey(msg.getType()));
            }
            ImmutableList.Builder<Alias> aliases = ImmutableList.builder();
            for (int i = 0; i < msg.getAliasesCount(); i++) {
                aliases.add(new Alias(msg.getAliases(i).getNamespace(), msg.getAliases(i).getValue()));
            }
            topic.setAliases(aliases.build());
            if (msg.getTitleCount() > 0) {
                topic.setTitle(msg.getTitle(0).getValue());
            }
            if (msg.getDescriptionCount() > 0) {
                topic.setDescription(msg.getDescription(0).getValue());
            }
            if (msg.getImageCount() > 0) {
                topic.setImage(msg.getImage(0));
            }
            if (msg.getThumbnailCount() > 0) {
                topic.setThumbnail(msg.getThumbnail(0));
            }
            topic.setMediaType(null);
            return topic;
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }

}
