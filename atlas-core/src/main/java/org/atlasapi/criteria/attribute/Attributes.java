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
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.MediaType;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class Attributes {

    public static final Attribute<Id> ID = idListAttribute("id", Identified.class);
    public static final Attribute<Publisher> SOURCE = EnumValuedAttribute.valueOf("source", Publisher.class, Identified.class, true);
    public static final Attribute<String> ALIASES_NAMESPACE = stringListAttribute("aliases.namespace", Identified.class);
    public static final Attribute<String> ALIASES_VALUE = stringListAttribute("aliases.value", Identified.class);
    
    public static final Attribute<Id> TOPIC_ID = idListAttribute("topics.topic.id", Identified.class);
    public static final Attribute<String> TOPIC_RELATIONSHIP = stringListAttribute("topics.relationship", Identified.class);
    public static final Attribute<Float> TOPIC_WEIGHTING = new FloatValuedAttribute("topics.weighting", Identified.class);
    public static final Attribute<Boolean> TOPIC_SUPERVISED = new BooleanValuedAttribute("topics.supervised", Identified.class);
    
    public static final Attribute<Topic.Type> TOPIC_TYPE = EnumValuedAttribute.valueOf("type", Topic.Type.class, Topic.class, true);
    public static final Attribute<ContentType> CONTENT_TYPE = EnumValuedAttribute.valueOf("type", ContentType.class, Content.class, true);
    
	// Simple string-valued attributes
    public static final Attribute<Publisher> DESCRIPTION_PUBLISHER = new EnumValuedAttribute<Publisher>("publisher", Publisher.class, Content.class);
    public static final Attribute<String> DESCRIPTION_GENRE = stringListAttribute("genre", Content.class);
    public static final Attribute<String> DESCRIPTION_TAG = stringListAttribute("tag", Content.class);
    public static final Attribute<MediaType> DESCRIPTION_TYPE = new EnumValuedAttribute<MediaType>("mediaType", MediaType.class, Item.class);
	public static final Attribute<String> TOPICS = stringListAttribute("topics", Content.class);
	public static final Attribute<String> CHANNEL_GROUP_TYPE = stringListAttribute("type", ChannelGroup.class);
	public static final Attribute<String> CHANNEL_GROUP_CHANNEL_GENRES = stringListAttribute("channel_genres", ChannelGroup.class);

	//public static final Attribute<Boolean> ITEM_IS_LONG_FORM = new BooleanValuedAttribute("isLongForm", Item.class).allowShortMatches();
	
	//public static final Attribute<Enum<MimeType>> ENCODING_DATA_CONTAINER_FORMAT = new EnumValuedAttribute<MimeType>("dataContainerFormat", MimeType.class, Encoding.class).allowShortMatches();

	// enums
	//public static final Attribute<Enum<TransportType>> LOCATION_TRANSPORT_TYPE = new EnumValuedAttribute<TransportType>("transportType", TransportType.class, Location.class).allowShortMatches();

	// Simple integer-valued attributes
	//public static final Attribute<Integer> EPISODE_POSITION = integerAttribute("position", "episodeNumber",  Episode.class).allowShortMatches();
	
	//public static final Attribute<Integer> EPISODE_SEASON_POSITION = integerAttribute("seasonPosition", "seriesNumber",  Episode.class).allowShortMatches();
	
	//public static final Attribute<Integer> VERSION_DURATION = integerAttribute("duration", Version.class).allowShortMatches();
	//public static final Attribute<Enum<Publisher>> VERSION_PROVIDER = new EnumValuedAttribute<Publisher>("provider", Publisher.class, Version.class);

	// Time based attributes
	public static final Attribute<DateTime> BRAND_THIS_OR_CHILD_LAST_UPDATED = dateTimeAttribute("thisOrChildLastUpdated", Container.class).allowShortMatches();
	//public static final Attribute<String> BROADCAST_ON = stringAttribute("broadcastOn", Broadcast.class).allowShortMatches().withAlias("channel");
	
	//public static final Attribute<Boolean> LOCATION_AVAILABLE = new BooleanValuedAttribute("available", Location.class).allowShortMatches();

	//public static final Attribute<String> POLICY_AVAILABLE_COUNTRY = new StringValuedAttribute("availableCountries", Policy.class, true).allowShortMatches();
	
	public static final Attribute<String> TOPIC_NAMESPACE = stringAttribute("namespace", Topic.class);
	public static final Attribute<String> TOPIC_VALUE = stringAttribute("value", Topic.class);
	
	// For applications
    public static final Attribute<Publisher> SOURCE_READS = EnumValuedAttribute.valueOf("source.reads", Publisher.class, Identified.class, true);
    public static final Attribute<Publisher> SOURCE_WRITES = EnumValuedAttribute.valueOf("source.writes", Publisher.class, Identified.class, true);
    public static final Attribute<Publisher> SOURCE_REQUEST_SOURCE = EnumValuedAttribute.valueOf("source", Publisher.class, Identified.class, true);

	//For Channels
    public static final String BROADCASTER_PARAM = "broadcaster";
    public static final String AVAILABLE_FROM_PARAM = "available_from";
    public static final String MEDIA_TYPE_PARAM = "media_type";
    public static final String ORDER_BY_PARAM = "order_by";
	public static final Attribute<Publisher> BROADCASTER = EnumValuedAttribute.valueOf(BROADCASTER_PARAM, Publisher.class, Identified.class, true);
	public static final Attribute<Publisher> AVAILABLE_FROM = EnumValuedAttribute.valueOf(AVAILABLE_FROM_PARAM, Publisher.class, Identified.class, true);
	public static final Attribute<MediaType> MEDIA_TYPE = new EnumValuedAttribute<>(MEDIA_TYPE_PARAM, MediaType.class, Identified.class);
	public static final Attribute<String> ORDER_BY = stringAttribute(ORDER_BY_PARAM, Channel.class);

    // For filtering
    public static final Attribute<String> CONTENT_TITLE_PREFIX = stringAttribute("title", Content.class);
    public static final Attribute<String> GENRE = stringAttribute("genre", Container.class);
	public static final Attribute<String> PRICE_ORDERING = stringAttribute("price.order", Container.class);

    private static List<Attribute<?>> ALL_ATTRIBUTES = 
		ImmutableList.<Attribute<?>>of(DESCRIPTION_TAG,
								    DESCRIPTION_GENRE,
								    DESCRIPTION_PUBLISHER,
								    DESCRIPTION_TYPE,
								    BRAND_THIS_OR_CHILD_LAST_UPDATED,
								    TOPIC_NAMESPACE,
								    TOPIC_VALUE/*,
								    VERSION_DURATION,
								    VERSION_PROVIDER,
								    BROADCAST_ON,
								    LOCATION_TRANSPORT_TYPE,
								    POLICY_AVAILABLE_COUNTRY,
								    EPISODE_POSITION,
								    EPISODE_SEASON_POSITION,
								    LOCATION_AVAILABLE,
								    ENCODING_DATA_CONTAINER_FORMAT,
								    ITEM_IS_LONG_FORM*/);
	
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

	
	private static void addToTable(Map<String, Attribute<?>> table, String key, Attribute<?> attribute) {
		if (table.containsKey(key)) {
			throw new IllegalArgumentException("Duplicate name: " + key);
		}
		table.put(key, attribute);
		
	}
	
	private static StringValuedAttribute stringAttribute(String name, Class<? extends Identified> target) {
		return new StringValuedAttribute(name, target);
	}
	
	private static IntegerValuedAttribute integerAttribute(String name,  String javaAttribute, Class<? extends Identified> target) {
		IntegerValuedAttribute attribute = new IntegerValuedAttribute(name, target);
		attribute.withJavaAttribute(javaAttribute);
		return attribute;
	}
	
	private static IntegerValuedAttribute integerAttribute(String name, Class<? extends Identified> target) {
		return new IntegerValuedAttribute(name, target);
	}
	
	private static DateTimeValuedAttribute dateTimeAttribute(String name, Class<? extends Identified> target) {
		return new DateTimeValuedAttribute(name, target);
	}

	private static StringValuedAttribute stringListAttribute(String name, Class<? extends Identified> target) {
		return new StringValuedAttribute(name, target, true);
	}

	private static IdAttribute idListAttribute(String name, Class<? extends Identified> target) {
	    return new IdAttribute(name, target, true);
	}
}
