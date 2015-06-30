package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Content.Builder;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

public final class ContentSerializer implements Serializer<Content, ContentProtos.Content> {

    private static final Set<Class<? extends Content>> supportedTypes = supportedTypes();

    private static Set<Class<? extends Content>> supportedTypes() {
        ImmutableSet.Builder<Class<? extends Content>> builder = ImmutableSet.builder();
        builder
            .add(Episode.class)
            .add(Item.class)
            .add(Brand.class)
            .add(Series.class)
            .add(Song.class)
            .add(Clip.class)
            .add(Film.class);
        return builder.build();
    }

    private ImmutableBiMap<String, Class<? extends Content>> typeNameMap;
    private final ContentSerializationVisitor serializationVisitor;

    public ContentSerializer(ContentSerializationVisitor visitor) {
        this.serializationVisitor = checkNotNull(visitor);
        ImmutableBiMap.Builder<String, Class<? extends Content>> typeNameMap = ImmutableBiMap.builder();
        for (Class<? extends Content> type : supportedTypes) {
            typeNameMap.put(typeString(type), type);
        }
        this.typeNameMap = typeNameMap.build();
    }

    @Override
    public ContentProtos.Content serialize(Content content) {
        String type = typeString(content.getClass());
        checkArgument(typeNameMap.containsKey(type), "Unsupported type: " + type);
        Builder builder = content.accept(serializationVisitor);
        return builder.build();
    }

    static String typeString(Class<?> cls) {
        return cls.getSimpleName().toLowerCase();
    }

    @Override
    public Content deserialize(ContentProtos.Content p) {
        try {
            String type = p.getType();
            Class<? extends Content> cls = typeNameMap.get(type);
            Content content = cls.newInstance();
            return content.accept(new ContentDeserializationVisitor(p));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
