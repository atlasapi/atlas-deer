package org.atlasapi.content;

import java.util.stream.Collectors;

import org.atlasapi.entity.DescribedSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class ContentGroupSerializer<T extends ContentGroup> {

    private final DescribedSerializer<ContentGroup> describedSerializer = new DescribedSerializer<>();
    private final ContentRefSerializer contentRefSerializer = new ContentRefSerializer(null);

    public ContentGroupSerializer() {
    }

    public CommonProtos.ContentGroup serialize(T contentGroup) {
        CommonProtos.ContentGroup.Builder builder = CommonProtos.ContentGroup.newBuilder();

        builder.setDescribed(describedSerializer.serialize(contentGroup));

        if (contentGroup.getType() != null) {
            builder.setType(contentGroup.getType().toString());
        }
        if (contentGroup.getContents() != null) {
            builder.addAllContent(contentGroup.getContents().stream()
                    .map(content -> contentRefSerializer.serialize(content).build())
                    .collect(Collectors.toList()));
        }

        return builder.build();
    }

    public T deserialize(CommonProtos.ContentGroup msg, T target) {
        describedSerializer.deserialize(msg.getDescribed(), target);

        if(msg.hasType()) {
            target.setType(ContentGroup.Type.valueOf(msg.getType()));
        }
        target.setContents(msg.getContentList().stream()
                .map(contentRefSerializer::deserialize)
                .collect(Collectors.toList()));

        return target;
    }
}
