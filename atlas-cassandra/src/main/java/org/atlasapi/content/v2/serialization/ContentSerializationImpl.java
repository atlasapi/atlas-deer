package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.Content;

public class ContentSerializationImpl implements ContentSerialization {

    private final ContentSerializer serializer;
    private final ContentDeserializer deserializer;

    public ContentSerializationImpl() {
        this.serializer = new ContentSerializerImpl();
        this.deserializer = new ContentDeserializerImpl();
    }

    @Override
    public org.atlasapi.content.v2.model.Content serialize(Content content) {
        return serializer.serialize(content);
    }

    @Override
    public Content deserialize(org.atlasapi.content.v2.model.Content internal) {
        return deserializer.deserialize(internal);
    }

}
