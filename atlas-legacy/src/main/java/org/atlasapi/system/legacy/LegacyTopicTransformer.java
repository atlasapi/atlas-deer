package org.atlasapi.system.legacy;

import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Topic;

import com.google.common.collect.ImmutableList;

public class LegacyTopicTransformer extends DescribedLegacyResourceTransformer<Topic, org.atlasapi.topic.Topic> {

    @Override
    protected org.atlasapi.topic.Topic createDescribed(Topic input) {
        org.atlasapi.topic.Topic topic = new org.atlasapi.topic.Topic();
        topic.setType(transformEnum(input.getType(), org.atlasapi.topic.Topic.Type.class));
        topic.setTitle(input.getTitle());
        topic.setDescription(input.getDescription());
        topic.setPublisher(input.getPublisher());
        topic.setNamespace(input.getNamespace());
        topic.setValue(input.getValue());
        return topic;
    }

    @Override
    protected Iterable<Alias> moreAliases(Topic input) {
        return ImmutableList.of(new Alias(input.getNamespace(), input.getValue()));
    }

}
