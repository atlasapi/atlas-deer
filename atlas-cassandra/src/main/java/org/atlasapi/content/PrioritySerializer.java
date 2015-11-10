package org.atlasapi.content;

import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class PrioritySerializer implements Serializer<Priority, CommonProtos.Priority> {

    @Override
    public CommonProtos.Priority serialize(Priority source) {
        CommonProtos.Priority.Builder builder = CommonProtos.Priority.newBuilder();

        if(source.getReasons() != null) {
            if (source.getReasons().getPositive() != null) {
                for (String reason : source.getReasons().getPositive()) {
                    builder.addPositiveReasons(reason);
                }
            }
            if (source.getReasons().getNegative() != null) {
                for (String reason : source.getReasons().getNegative()) {
                    builder.addNegativeReasons(reason);
                }
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
        target.setReasons(new PriorityScoreReasons(
                serialized.getPositiveReasonsList(),
                serialized.getNegativeReasonsList()
        ));

        return target;
    }

}
