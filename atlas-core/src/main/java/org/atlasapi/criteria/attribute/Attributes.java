package org.atlasapi.criteria.attribute;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
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

    public static final Attribute<Id> ID = IdAttribute.list(
            "id",
            Identified.class
    );
    public static final Attribute<Publisher> SOURCE = EnumAttribute.list(
            "source",
            Publisher.class,
            Identified.class
    );
    public static final Attribute<String> ALIASES_NAMESPACE = StringAttribute.list(
            "aliases.namespace",
            Identified.class
    );
    public static final Attribute<String> ALIASES_VALUE = StringAttribute.list(
            "aliases.value",
            Identified.class
    );
    public static final Attribute<String> LOCATIONS_ALIASES_NAMESPACE = StringAttribute.list(
            "locations.aliases.namespace",
            Identified.class
    );
    public static final Attribute<String> LOCATIONS_ALIASES_VALUE = StringAttribute.list(
            "locations.aliases.value",
            Identified.class
    );
    public static final Attribute<Id> REGION = IdAttribute.list(
            "region",
            Region.class
    );
    public static final Attribute<Id> PLATFORM = IdAttribute.single(
            "platform",
            Identified.class
    );
    public static final Attribute<Id> DOWNWEIGH = IdAttribute.list(
            "downweigh",
            Identified.class
    );

    /**
     * This attribute is intended for use only by the topic endpoint. This cannot be used on the
     * content endpoint.
     */

    public static final Attribute<Id> TOPIC_ID = IdAttribute.list(
            "tags.topic.id",
            "topics.topic.id",
            Identified.class
    );

    /**
     * This attribute is intended for use on by the content endpoint. This should not be used on
     * the topic endpoint.
     */
    public static final Attribute<String> SEARCH_TOPIC_ID = StringAttribute.list(
            "tags.topic.id",
            "topics.topic.id",
            Identified.class
    );

    public static final Attribute<String> TAG_RELATIONSHIP = StringAttribute.list(
            "tags.relationship",
            "topics.relationship",
            Identified.class
    );
    public static final Attribute<Float> TAG_WEIGHTING = FloatAttribute.single(
            "tags.weighting",
            "topics.weighting",
            Identified.class
    );
    public static final Attribute<Boolean> TAG_SUPERVISED = BooleanAttribute.single(
            "tags.supervised",
            "topics.supervised",
            Identified.class
    );
    public static final Attribute<Topic.Type> TOPIC_TYPE = EnumAttribute.list(
            "type",
            Topic.Type.class,
            Topic.class
    );
    public static final Attribute<ContentType> CONTENT_TYPE = EnumAttribute.list(
            "type",
            ContentType.class,
            Content.class
    );
    public static final Attribute<String> CHANNEL_GROUP_TYPE = StringAttribute.list(
            "type",
            ChannelGroup.class
    );
    public static final Attribute<String> CHANNEL_GROUP_CHANNEL_GENRES = StringAttribute.list(
            "channel_genres",
            ChannelGroup.class
    );
    public static final Attribute<String> CHANNEL_GROUP_IP_CHANNELS = StringAttribute.list(
            "ip_only",
            ChannelGroup.class
    );
    public static final Attribute<String> CHANNEL_GROUP_DTT_CHANNELS = StringAttribute.list(
            "dtt_only",
            ChannelGroup.class
    );
    public static final Attribute<String> Q = StringAttribute.single(
            "q",
            Identified.class
    );
    public static final Attribute<Float> TITLE_BOOST = FloatAttribute.single(
            "title_boost",
            Identified.class
    );
    public static final Attribute<String> ORDER_BY = StringAttribute.single(
            "order_by",
            Identified.class
    );
    public static final Attribute<Float> BROADCAST_WEIGHT = FloatAttribute.single(
            "broadcastWeight",
            Identified.class
    );
    public static final Attribute<Id> BRAND_ID = IdAttribute.single(
            "brand.id",
            Identified.class
    );
    public static final Attribute<Id> EPISODE_BRAND_ID = IdAttribute.single(
            "episode.brand.id",
            Identified.class
    );
    public static final Attribute<Id> SERIES_ID = IdAttribute.single(
            "series.id",
            Identified.class
    );
    public static final Attribute<String> ACTIONABLE_FILTER_PARAMETERS = StringAttribute.list(
            "actionableFilterParameters",
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

    public static final Attribute<Boolean> ADVERTISED_ON = BooleanAttribute.single(
            ADVERTISED_FROM_PARAM,
            Identified.class
    );
    public static final Attribute<Publisher> BROADCASTER = EnumAttribute.list(
            BROADCASTER_PARAM,
            Publisher.class,
            Identified.class
    );
    public static final Attribute<Publisher> AVAILABLE_FROM = EnumAttribute.list(
            AVAILABLE_FROM_PARAM,
            Publisher.class,
            Identified.class
    );
    public static final Attribute<MediaType> MEDIA_TYPE = EnumAttribute.single(
            MEDIA_TYPE_PARAM,
            MediaType.class,
            Identified.class
    );
    public static final Attribute<String> ORDER_BY_CHANNEL = StringAttribute.single(
            ORDER_BY_PARAM,
            Channel.class
    );

    public static final Attribute<String> REFRESH_CACHE = StringAttribute.single(
            REFRESH_CACHE_PARAM,
            Channel.class
    );
    public static final Attribute<String> CHANNEL_GROUP_REFRESH_CACHE = StringAttribute.single(
            REFRESH_CACHE_PARAM,
            ChannelGroup.class
    );

    // For filtering
    public static final Attribute<String> CONTENT_TITLE_PREFIX = StringAttribute.single(
            "title",
            "parentFlattenedTitle",
            Content.class
    )
            .withAlias("parentFlattenedTitle");

    public static final Attribute<String> GENRE = StringAttribute.list(
            "genre",
            Container.class
    );

    public static final Attribute<Id> CONTENT_GROUP = IdAttribute.list(
            "contentGroups",
            "contentGroups",
            Content.class
    )
            .withAlias("contentGroups");

    public static final Attribute<Specialization> SPECIALIZATION = EnumAttribute.single(
            "specialization",
            Specialization.class,
            Described.class
    );
}
