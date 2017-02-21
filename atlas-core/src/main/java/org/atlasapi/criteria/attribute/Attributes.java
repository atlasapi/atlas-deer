/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.criteria.attribute;

import java.util.List;
import java.util.Map;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Described;
import org.atlasapi.content.Item;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;

public class Attributes {

    private Attributes() {
    }

    public static final Attribute<Id> ID = IdAttribute.list("id", Identified.class);
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
    public static final Attribute<Id> REGION = IdAttribute.list("region", Region.class);

    public static final Attribute<Id> PLATFORM = IdAttribute.list("platform", Platform.class);

    public static final Attribute<Id> DOWNWEIGH = IdAttribute.list("downweigh", Channel.class);

    public static final Attribute<Id> TOPIC_ID = IdAttribute.list(
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

    // Simple string-valued attributes
    public static final Attribute<Publisher> DESCRIPTION_PUBLISHER = EnumAttribute.single(
            "publisher",
            Publisher.class,
            Content.class
    );
    public static final Attribute<String> DESCRIPTION_GENRE = StringAttribute.list(
            "genre",
            Content.class
    );
    public static final Attribute<String> DESCRIPTION_TAG = StringAttribute.list(
            "tag",
            Content.class
    );
    public static final Attribute<MediaType> DESCRIPTION_TYPE = EnumAttribute.single(
            "mediaType",
            MediaType.class,
            Item.class
    );
    public static final Attribute<String> TAGS = StringAttribute.list(
            "tags",
            "topics",
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

    // Time based attributes
    public static final Attribute<DateTime> BRAND_THIS_OR_CHILD_LAST_UPDATED = DateTimeAttribute
            .single(
            "thisOrChildLastUpdated",
            Container.class
    ).allowShortMatches();


    public static final Attribute<String> TOPIC_NAMESPACE = StringAttribute.single(
            "namespace",
            Topic.class
    );
    public static final Attribute<String> TOPIC_VALUE = StringAttribute.single(
            "value",
            Topic.class
    );

    // For applications
    public static final Attribute<Publisher> SOURCE_READS = EnumAttribute.list(
            "source.reads",
            Publisher.class,
            Identified.class
    );
    public static final Attribute<Publisher> SOURCE_WRITES = EnumAttribute.list(
            "source.writes",
            Publisher.class,
            Identified.class
    );
    public static final Attribute<Publisher> SOURCE_REQUEST_SOURCE = EnumAttribute.list(
            "source",
            Publisher.class,
            Identified.class
    );

    //For Channels
    public static final String BROADCASTER_PARAM = "broadcaster";
    public static final String AVAILABLE_FROM_PARAM = "available_from";
    public static final String MEDIA_TYPE_PARAM = "media_type";
    public static final String ORDER_BY_PARAM = "order_by";
    public static final String ADVERTISED_FROM_PARAM = "advertised";
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

    // For filtering
    public static final Attribute<String> CONTENT_TITLE_PREFIX = StringAttribute.single(
            "title",
            "parentFlattenedTitle",
            Content.class
    ).withAlias("parentFlattenedTitle");

    public static final Attribute<String> GENRE = StringAttribute.list("genre", Container.class);
    public static final Attribute<Id> CONTENT_GROUP = IdAttribute.list(
            "contentGroups",
            "contentGroups",
            Content.class
    ).withAlias("contentGroups");
    public static final Attribute<Specialization> SPECIALIZATION = EnumAttribute.single(
            "specialization",
            Specialization.class,
            Described.class
    );

    private static List<Attribute<?>> ALL_ATTRIBUTES =
            ImmutableList.of(
                    DESCRIPTION_TAG,
                    DESCRIPTION_GENRE,
                    DESCRIPTION_PUBLISHER,
                    DESCRIPTION_TYPE,
                    BRAND_THIS_OR_CHILD_LAST_UPDATED,
                    TOPIC_NAMESPACE,
                    TOPIC_VALUE
            );

    public static final Map<String, Attribute<?>> lookup = lookupTable();

    public static Attribute<?> lookup(String name) {
        return lookup.get(name);
    }

    private static Map<String, Attribute<?>> lookupTable() {
        Map<String, Attribute<?>> table = Maps.newHashMap();

        for (Attribute<?> attribute : ALL_ATTRIBUTES) {
            addToTable(table, attribute.externalName(), attribute);
            if (attribute.hasAlias()) {
                table.put(attribute.alias(), attribute);
            }
        }
        return table;
    }

    private static void addToTable(Map<String, Attribute<?>> table, String key,
            Attribute<?> attribute) {
        if (table.containsKey(key)) {
            throw new IllegalArgumentException("Duplicate name: " + key);
        }
        table.put(key, attribute);

    }
}
