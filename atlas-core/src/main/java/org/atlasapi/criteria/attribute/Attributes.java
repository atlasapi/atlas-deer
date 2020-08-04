package org.atlasapi.criteria.attribute;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Described;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;

public class Attributes {

    private Attributes() {
    }

    public static final Attribute<Id> ID = IdAttribute.create(
            "id",
            Identified.class
    );
    public static final Attribute<Publisher> SOURCE = EnumAttribute.create(
            "source",
            Publisher.class,
            Identified.class
    );
    public static final Attribute<String> ALIASES_NAMESPACE = StringAttribute.create(
            "aliases.namespace",
            Identified.class
    );
    public static final Attribute<String> ALIASES_VALUE = StringAttribute.create(
            "aliases.value",
            Identified.class
    );
    public static final Attribute<String> LOCATIONS_ALIASES_NAMESPACE = StringAttribute.create(
            "locations.aliases.namespace",
            Identified.class
    );
    public static final Attribute<String> LOCATIONS_ALIASES_VALUE = StringAttribute.create(
            "locations.aliases.value",
            Identified.class
    );
    public static final Attribute<Id> REGION = IdAttribute.create(
            "region",
            Region.class
    );
    public static final Attribute<Id> PLATFORM = IdAttribute.create(
            "platform",
            Platform.class
    );
    public static final Attribute<Id> DOWNWEIGH = IdAttribute.create(
            "downweigh",
            Identified.class
    );

    /**
     * This attribute is intended for use only by the topic endpoint. This cannot be used on the
     * content endpoint.
     */

    public static final Attribute<Id> TOPIC_ID = IdAttribute.create(
            "tags.topic.id",
            "topics.topic.id",
            Identified.class
    );

    /**
     * This attribute is intended for use on by the content endpoint. This should not be used on
     * the topic endpoint.
     */
    public static final Attribute<String> SEARCH_TOPIC_ID = StringAttribute.create(
            "tags.topic.id",
            "topics.topic.id",
            Identified.class
    );

    public static final Attribute<String> TAG_RELATIONSHIP = StringAttribute.create(
            "tags.relationship",
            "topics.relationship",
            Identified.class
    );
    public static final Attribute<Float> TAG_WEIGHTING = FloatAttribute.create(
            "tags.weighting",
            "topics.weighting",
            Identified.class
    );
    public static final Attribute<Boolean> TAG_SUPERVISED = BooleanAttribute.create(
            "tags.supervised",
            "topics.supervised",
            Identified.class
    );
    public static final Attribute<Topic.Type> TOPIC_TYPE = EnumAttribute.create(
            "type",
            Topic.Type.class,
            Topic.class
    );
    public static final Attribute<ContentType> CONTENT_TYPE = EnumAttribute.create(
            "type",
            ContentType.class,
            Content.class
    );
    public static final Attribute<String> CHANNEL_GROUP_TYPE = StringAttribute.create(
            "type",
            ChannelGroup.class
    );
    public static final Attribute<String> CHANNEL_GROUP_CHANNEL_GENRES = StringAttribute.create(
            "channel_genres",
            ChannelGroup.class
    );
    public static final Attribute<Id> CHANNEL_GROUP_IP_CHANNELS = IdAttribute.create(
            "ip_only",
            ChannelGroup.class
    );
    public static final Attribute<Id> CHANNEL_GROUP_DTT_CHANNELS = IdAttribute.create(
            "dtt_only",
            ChannelGroup.class
    );
    public static final Attribute<Id> CHANNEL_GROUP_IDS = IdAttribute.create(
            "ids",
            ChannelGroup.class
    );
    public static final Attribute<String> Q = StringAttribute.create(
            "q",
            Identified.class
    );
    public static final Attribute<Float> TITLE_BOOST = FloatAttribute.create(
            "title_boost",
            Identified.class
    );
    public static final Attribute<String> ORDER_BY = StringAttribute.create(
            "order_by",
            Identified.class
    );
    public static final Attribute<Float> BROADCAST_WEIGHT = FloatAttribute.create(
            "broadcastWeight",
            Identified.class
    );
    public static final Attribute<Id> BRAND_ID = IdAttribute.create(
            "brand.id",
            Identified.class
    );
    public static final Attribute<Id> EPISODE_BRAND_ID = IdAttribute.create(
            "episode.brand.id",
            Identified.class
    );
    public static final Attribute<Id> SERIES_ID = IdAttribute.create(
            "series.id",
            Identified.class
    );
    public static final Attribute<String> ACTIONABLE_FILTER_PARAMETERS = StringAttribute.create(
            "actionableFilterParameters",
            Identified.class
    );
    public static final Attribute<Boolean> HIGHER_READ_CONSISTENCY = BooleanAttribute.create(
            "higherReadConsistency",
            Identified.class
    );

    // For Channels
    public static final String BROADCASTER_PARAM = "broadcaster";
    public static final String AVAILABLE_FROM_PARAM = "available_from";
    public static final String MEDIA_TYPE_PARAM = "media_type";
    public static final String ORDER_BY_PARAM = "order_by";
    public static final String ADVERTISED_FROM_PARAM = "advertised";
    public static final String ALIASES_NAMESPACE_PARAM = "aliases.namespace";
    public static final String ALIASES_VALUE_PARAM = "aliases.value";
    public static final String REFRESH_CACHE_PARAM = "refresh_cache";

    public static final Attribute<Boolean> ADVERTISED_ON = BooleanAttribute.create(
            ADVERTISED_FROM_PARAM,
            Identified.class
    );
    public static final Attribute<Publisher> BROADCASTER = EnumAttribute.create(
            BROADCASTER_PARAM,
            Publisher.class,
            Identified.class
    );
    public static final Attribute<Publisher> AVAILABLE_FROM = EnumAttribute.create(
            AVAILABLE_FROM_PARAM,
            Publisher.class,
            Identified.class
    );
    public static final Attribute<MediaType> MEDIA_TYPE = EnumAttribute.create(
            MEDIA_TYPE_PARAM,
            MediaType.class,
            Identified.class
    );
    public static final Attribute<String> ORDER_BY_CHANNEL = StringAttribute.create(
            ORDER_BY_PARAM,
            Channel.class
    );

    public static final Attribute<String> REFRESH_CACHE = StringAttribute.create(
            REFRESH_CACHE_PARAM,
            Channel.class
    );
    public static final Attribute<String> CHANNEL_GROUP_REFRESH_CACHE = StringAttribute.create(
            REFRESH_CACHE_PARAM,
            ChannelGroup.class
    );

    // For filtering
    public static final Attribute<String> CONTENT_TITLE_PREFIX = StringAttribute.create(
            "title",
            "parentFlattenedTitle",
            Content.class
    );

    public static final Attribute<String> GENRE = StringAttribute.create(
            "genre",
            Container.class
    );

    public static final Attribute<Id> CONTENT_GROUP = IdAttribute.create(
            "contentGroups",
            "contentGroups",
            Content.class
    );

    public static final Attribute<Specialization> SPECIALIZATION = EnumAttribute.create(
            "specialization",
            Specialization.class,
            Described.class
    );
}
