package org.atlasapi.content;

import java.util.stream.Collectors;

import org.atlasapi.entity.DescribedSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class ContentGroupSerializer<T extends ContentGroup> {

    public CommonProtos.ContentGroup serialize(T contentGroup) {
        CommonProtos.ContentGroup.Builder builder = CommonProtos.ContentGroup.newBuilder();

        builder.setDescribed(new DescribedSerializer<ContentGroup>().serialize(contentGroup));

        if (contentGroup.getType() != null) {
            builder.setType(contentGroup.getType().toString());
        }
        if (contentGroup.getContents() != null) {
            builder.addAllContent(contentGroup.getContents().stream()
                    .map(content -> new ContentRefSerializer(null).serialize(content).build())
                    .collect(Collectors.toList()));
        }

        return builder.build();
    }

    public T deserialize(CommonProtos.ContentGroup msg, T target) {
        new DescribedSerializer<ContentGroup>().deserialize(msg.getDescribed(), target);

        if(msg.hasType()) {
            target.setType(ContentGroup.Type.valueOf(msg.getType()));
        }
        target.setContents(msg.getContentList().stream()
                .map(ref -> new ContentRefSerializer(null).deserialize(ref))
                .collect(Collectors.toList()));

        return target;
    }
}
