package org.atlasapi.content;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public enum ContentType {

    BRAND(Brand.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitBrand(this);
        }
    },
    SERIES(Series.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitSeries(this);
        }
    },
    ITEM(Item.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitItem(this);
        }
    },
    EPISODE(Episode.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitEpisode(this);
        }
    },
    FILM(Film.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitFilm(this);
        }
    },
    SONG(Song.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitSong(this);
        }
    },
    CLIP(Clip.class) {
        @Override
        public <V> V accept(Visitor<V> v) {
            return v.visitClip(this);
        }
    };

    private Class<? extends Content> cls;

    ContentType(Class<? extends Content> cls) {
        this.cls = cls;
    }

    public abstract <V> V accept(Visitor<V> v);

    public String getKey() {
        return cls.getSimpleName().toLowerCase();
    }

    public Class<? extends Content> getContentClass() {
        return cls;
    }

    @Override
    public String toString() {
        return getKey();
    }

    private static final Function<ContentType, String> TO_KEY =
            new Function<ContentType, String>() {

                @Override
                public String apply(ContentType input) {
                    return input.getKey();
                }
            };

    public static final Function<ContentType, String> toKey() {
        return TO_KEY;
    }

    private static final ImmutableSet<ContentType> ALL
            = ImmutableSet.copyOf(ContentType.values());

    public static final ImmutableSet<ContentType> all() {
        return ALL;
    }

    private static final OptionalMap<String, ContentType> KEY_INDEX
            = ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), toKey()));

    public static final Optional<ContentType> fromKey(String key) {
        return KEY_INDEX.get(key);
    }

    private static final Function<String, Optional<ContentType>> FROM_KEY
            = Functions.forMap(KEY_INDEX);

    public static final Function<String, Optional<ContentType>> fromKey() {
        return FROM_KEY;
    }

    private static final Function<ContentType, Class<? extends Content>> TO_CLASS =
            new Function<ContentType, Class<? extends Content>>() {

                @Override
                public Class<? extends Content> apply(ContentType input) {
                    return input.getContentClass();
                }
            };

    public static final Function<ContentType, Class<? extends Content>> toContentClass() {
        return TO_CLASS;
    }

    private static final OptionalMap<Class<? extends Content>, ContentType> CLASS_INDEX
            = ImmutableOptionalMap.fromMap(Maps.uniqueIndex(all(), toContentClass()));

    public static final Optional<ContentType> fromContent(Content content) {
        return CLASS_INDEX.get(content.getClass());
    }

    private static final Function<Class<? extends Content>, Optional<ContentType>> FROM_CLASS
            = Functions.forMap(CLASS_INDEX);

    public static final Function<Class<? extends Content>, Optional<ContentType>> fromContentClass() {
        return FROM_CLASS;
    }

    public interface Visitor<T> {

        T visitBrand(ContentType contentType);

        T visitClip(ContentType contentType);

        T visitSong(ContentType contentType);

        T visitFilm(ContentType contentType);

        T visitEpisode(ContentType contentType);

        T visitItem(ContentType contentType);

        T visitSeries(ContentType contentType);

    }
}
