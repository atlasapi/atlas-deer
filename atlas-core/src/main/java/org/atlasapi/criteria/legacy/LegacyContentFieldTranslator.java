package org.atlasapi.criteria.legacy;

import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

public class LegacyContentFieldTranslator {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    public static LegacyTranslation translate(String oldClusterFieldName) {
        switch (oldClusterFieldName) {
        case "id":
            return LegacyTranslation.of(CONTENT_MAPPING.getId());
        case "type":
            return LegacyTranslation.of(CONTENT_MAPPING.getType());
        case "source":
            return LegacyTranslation.of(CONTENT_MAPPING.getSource().getKey());
        case "aliases.namespace":
            return LegacyTranslation.of(CONTENT_MAPPING.getAliases().getNamespace());
        case "aliases.value":
            return LegacyTranslation.of(CONTENT_MAPPING.getAliases().getValue());
        case "title":
            return LegacyTranslation.of(CONTENT_MAPPING.getTitle());
        case "flattenedTitle":
            return LegacyTranslation.of(CONTENT_MAPPING.getTitle());
        case "parentTitle":
            return LegacyTranslation.of(CONTENT_MAPPING.getContainer().getTitle());
        case "parentFlattenedTitle":
            return LegacyTranslation.of(CONTENT_MAPPING.getContainer().getTitle());
        case "specialization":
            return LegacyTranslation.of(CONTENT_MAPPING.getSpecialization());
        case "broadcasts.id":
            return LegacyTranslation.silentlyIgnore();
        case "broadcasts.channel":
            return LegacyTranslation.of(CONTENT_MAPPING.getBroadcasts().getBroadcastOn());
        case "broadcasts.transmissionTime":
            return LegacyTranslation.of(CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime());
        case "broadcasts.transmissionEndTime":
            return LegacyTranslation.of(CONTENT_MAPPING.getBroadcasts().getTransmissionEndTime());
        case "broadcasts.transmissionTimeInMillis":
            return LegacyTranslation.silentlyIgnore();
        case "broadcasts.repeat":
            return LegacyTranslation.silentlyIgnore();
        case "locations.availabilityTime":
            return LegacyTranslation.of(CONTENT_MAPPING.getLocations().getAvailabilityStart());
        case "locations.availabilityEndTime":
            return LegacyTranslation.of(CONTENT_MAPPING.getLocations().getAvailabilityEnd());
        case "locations.aliases.value":
            return LegacyTranslation.of(CONTENT_MAPPING.getLocations().getAliases().getValue());
        case "locations.aliases.namespace":
            return LegacyTranslation.of(CONTENT_MAPPING.getLocations().getAliases().getNamespace());
        case "topics.topic.id":
            return LegacyTranslation.of(CONTENT_MAPPING.getTags().getId());
        case "topics.supervised":
            return LegacyTranslation.of(CONTENT_MAPPING.getTags().getSupervised());
        case "topics.weighting":
            return LegacyTranslation.of(CONTENT_MAPPING.getTags().getWeighting());
        case "topics.relationship":
            return LegacyTranslation.of(CONTENT_MAPPING.getTags().getRelationship());
        case "hasChildren":
            return LegacyTranslation.silentlyIgnore();
        case "genre":
            return LegacyTranslation.of(CONTENT_MAPPING.getGenres());
        case "price.currency":
            return LegacyTranslation.of(CONTENT_MAPPING.getLocations().getPricing().getCurrency());
        case "price.value":
            return LegacyTranslation.of(CONTENT_MAPPING.getLocations().getPricing().getPrice());
        case "age":
            return LegacyTranslation.of(CONTENT_MAPPING.getMaxAgeRating());
        case "contentGroups":
            return LegacyTranslation.silentlyIgnore();
        case "priority":
            return LegacyTranslation.silentlyIgnore();
        case "seriesNumber":
            return LegacyTranslation.of(CONTENT_MAPPING.getSeriesNumber());
        case "episodeNumber":
            return LegacyTranslation.of(CONTENT_MAPPING.getEpisodeNumber());
        case "brand":
            return LegacyTranslation.of(CONTENT_MAPPING.getBrandId());
        case "series":
            return LegacyTranslation.of(CONTENT_MAPPING.getSeriesId());
        case "sortKey":
            return LegacyTranslation.silentlyIgnore();
        case "canonicalId":
            return LegacyTranslation.of(CONTENT_MAPPING.getCanonicalId());
        default:
            return LegacyTranslation.unknownField();
        }
    }
}
