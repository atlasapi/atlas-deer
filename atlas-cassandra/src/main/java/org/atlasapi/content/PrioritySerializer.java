package org.atlasapi.content;

import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class PrioritySerializer implements Serializer<Priority, CommonProtos.Priority> {

    @Override
    public CommonProtos.Priority serialize(Priority source) {
        CommonProtos.Priority.Builder builder = CommonProtos.Priority.newBuilder();

        if(source.getReasons() != null) {
            for (String reason : source.getReasons()) {
                builder.addReason(reason);
            }
        }
        if(source.getPriority() != null) {
            builder.setPriority(source.getPriority());
        }

        return builder.build();
    }

    @Override
    public Priority deserialize(CommonProtos.Priority serialized) {
        Priority target = new Priority();

        if(serialized.hasPriority()) {
            target.setPriority(serialized.getPriority());
        }
        target.setReasons(serialized.getReasonList());

        return target;
    }

}
