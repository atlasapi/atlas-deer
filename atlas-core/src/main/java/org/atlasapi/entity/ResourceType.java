package org.atlasapi.entity;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Platform;
import org.atlasapi.content.Content;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Player;
import org.atlasapi.segment.Segment;
import org.atlasapi.topic.Topic;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;


public enum ResourceType {
    CONTENT(Content.class),
    TOPIC(Topic.class),
    SEGMENT(Segment.class),
    CHANNEL(Channel.class),
    CHANNEL_GROUP(ChannelGroup.class),
    SERVICE(Platform.class),
    PLAYER(Player.class),
    ;

    private Class<? extends Identified> cls;

    ResourceType(Class<? extends Identified> cls) {
        this.cls = cls;
    }
    
    public String getKey() {
        return cls.getSimpleName().toLowerCase();
    }
    
    public Class<? extends Identified> getIdentifiedClass() {
        return cls;
    }
    
    @Override
    public String toString() {
        return getKey();
    }
    
    private static final Function<ResourceType, String> TO_KEY =
            new Function<ResourceType, String>() {
                @Override
                public String apply(ResourceType input) {
                    return input.getKey();
                }
            };
            
    public static final Function<ResourceType, String> toKey() {
        return TO_KEY;
    }

    private static final ImmutableSet<ResourceType> ALL
        = ImmutableSet.copyOf(ResourceType.values());
    
    public static final ImmutableSet<ResourceType> all() {
        return ALL;
    }
    
    private static final OptionalMap<String, ResourceType> KEY_INDEX
        = ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), toKey())); 
            
    public static final Optional<ResourceType> fromKey(String key) {
        return KEY_INDEX.get(key);
    }
    
    private static final Function<String, Optional<ResourceType>> FROM_KEY
        = Functions.forMap(KEY_INDEX);
    
    public static final Function<String, Optional<ResourceType>> fromKey() {
        return FROM_KEY;
    }
    
    private static final Function<ResourceType, Class<? extends Identified>> TO_CLASS =
            new Function<ResourceType, Class<? extends Identified>>() {
                @Override
                public Class<? extends Identified> apply(ResourceType input) {
                    return input.getIdentifiedClass();
                }
            };
            
    public static final Function<ResourceType, Class<? extends Identified>> toIdentifiedClass() {
        return TO_CLASS;
    }
    
    private static final OptionalMap<Class<? extends Identified>, ResourceType> CLASS_INDEX
        = ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), toIdentifiedClass()));
    
    public static final Optional<ResourceType> fromIdentified(Identified Identified) {
        return CLASS_INDEX.get(Identified.getClass());
    }
    
    private static final Function<Class<? extends Identified>, Optional<ResourceType>> FROM_CLASS
        = Functions.forMap(CLASS_INDEX);
    
    public static final Function<Class<? extends Identified>, Optional<ResourceType>> fromIdentifiedClass() {
        return FROM_CLASS;
    }
    
}
