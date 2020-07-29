package org.atlasapi.criteria.attribute;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;

import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.mapping.TopicMapping;

public class TopicAttributes {

    private static final TopicMapping TOPIC_MAPPING = IndexMapping.getTopicMapping();

    private TopicAttributes() {
    }

    public static final Attribute<Id> ID = IdAttribute.create(
            "id",
            TOPIC_MAPPING.getId(),
            Identified.class
    );
    public static final Attribute<Publisher> SOURCE = EnumAttribute.create(
            "source",
            Publisher.class,
            TOPIC_MAPPING.getSource().getKey(),
            Identified.class
    );
    public static final Attribute<String> ALIASES_NAMESPACE = StringAttribute.create(
            "aliases.namespace",
            TOPIC_MAPPING.getAliases().getNamespace(),
            Identified.class
    );
    public static final Attribute<String> ALIASES_VALUE = StringAttribute.create(
            "aliases.value",
            TOPIC_MAPPING.getAliases().getValue(),
            Identified.class
    );
    public static final Attribute<Topic.Type> TOPIC_TYPE = EnumAttribute.create(
            "type",
            Topic.Type.class,
            TOPIC_MAPPING.getTopicType(),
            Topic.class
    );
}
