package org.atlasapi.entity;

import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentRefSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class ResourceRefSerializer implements Serializer<ResourceRef, CommonProtos.Reference> {

    private final ContentRefSerializer contentRefSerializer
            = new ContentRefSerializer(null);

    @Override
    public CommonProtos.Reference serialize(ResourceRef src) {
        //TODO other resource refs...
        return contentRefSerializer.serialize((ContentRef) src).build();
    }

    @Override
    public ResourceRef deserialize(CommonProtos.Reference dest) {
        return contentRefSerializer.deserialize(dest);
    }

}
