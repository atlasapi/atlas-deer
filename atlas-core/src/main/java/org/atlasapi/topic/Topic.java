package org.atlasapi.topic;

import java.util.Map;

import org.atlasapi.content.Described;
import org.atlasapi.entity.Aliased;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class Topic extends Described implements Sourced, Aliased {

    private Type type;
    private String namespace;
    private String value;

    public enum Type {
        SUBJECT("subject"),
        PERSON("person"),
        PLACE("place"),
        ARTIST("artist"),
        EVENT("event"),
        PRODUCT("product"),
        WORK("work"),
        GENRE("genre"),
        LABEL("label"),
        UNKNOWN("unknown");

        private final String key;

        Type(String key) {
            this.key = key;
        }

        @FieldName("key")
        public String key() {
            return key;
        }

        @Override
        public String toString() {
            return key;
        }

        @Deprecated
        public static Map<String, Type> TYPE_KEY_LOOKUP = Maps.uniqueIndex(
                ImmutableSet.copyOf(Type.values()),
                new Function<Type, String>() {

                    @Override
                    public String apply(Type input) {
                        return input.key;
                    }
                }
        );

        private static final Function<Type, String> TO_KEY =
                new Function<Type, String>() {

                    @Override
                    public String apply(Type input) {
                        return input.key();
                    }
                };

        public static final Function<Type, String> toKey() {
            return TO_KEY;
        }

        private static final ImmutableSet<Type> ALL =
                ImmutableSet.copyOf(Type.values());

        public static final ImmutableSet<Type> all() {
            return ALL;
        }

        private static final OptionalMap<String, Type> INDEX =
                ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), toKey()));

        private static final Function<String, Optional<Type>> FROM_KEY =
                Functions.forMap(INDEX);

        public static final Function<String, Optional<Type>> fromKey() {
            return FROM_KEY;
        }

        public static Type fromKey(String key) {
            return INDEX.get(key).orNull();
        }

        public static Type fromKey(String key, Type deflt) {
            return INDEX.get(key).or(deflt);
        }

    }

    public Topic() {
        this(null, null, null);
    }

    public Topic(long id) {
        this(Id.valueOf(id), null, null);
    }

    public Topic(Id id) {
        this(id, null, null);
    }

    public Topic(Id id, String namespace, String value) {
        setId(id);
        setMediaType(null);
        this.namespace = namespace;
        this.value = value;
    }

    public TopicRef toRef() {
        return new TopicRef(getId(), getSource());
    }

    public static Topic copyTo(Topic from, Topic to) {
        Described.copyTo(from, to);
        to.type = from.type;
        to.namespace = from.namespace;
        to.value = from.value;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Topic) {
            copyTo(this, (Topic) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Topic copy() {
        return copyTo(this, new Topic());
    }

    @Override
    public Topic createNew() {
        return new Topic();
    }

    @FieldName("type")
    public Type getType() {
        return this.type;
    }

    @FieldName("namespace")
    @Deprecated
    public String getNamespace() {
        return this.namespace;
    }

    @FieldName("value")
    @Deprecated
    public String getValue() {
        return this.value;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Deprecated
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Deprecated
    public void setValue(String value) {
        this.value = value;
    }

}
