package org.atlasapi.criteria.legacy;

import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.mapping.TopicMapping;

public class LegacyTopicFieldTranslator {

    private static final TopicMapping TOPIC_MAPPING = IndexMapping.getTopicMapping();

    public static LegacyTranslation translate(String oldClusterFieldName) {
        switch (oldClusterFieldName) {
        case "id":
            return LegacyTranslation.of(TOPIC_MAPPING.getId());
        case "type":
            return LegacyTranslation.of(TOPIC_MAPPING.getTopicType());
        case "source":
            return LegacyTranslation.of(TOPIC_MAPPING.getSource().getKey());
        case "aliases.namespace":
            return LegacyTranslation.of(TOPIC_MAPPING.getAliases().getNamespace());
        case "aliases.value":
            return LegacyTranslation.of(TOPIC_MAPPING.getAliases().getValue());
        case "title":
            return LegacyTranslation.of(TOPIC_MAPPING.getTitle());
        case "description":
            return LegacyTranslation.of(TOPIC_MAPPING.getDescription());
        default:
            return LegacyTranslation.unknownField();
        }
    }
}
