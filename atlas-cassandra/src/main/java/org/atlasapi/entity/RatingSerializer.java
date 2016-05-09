package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.source.Sources;

public class RatingSerializer {
    public CommonProtos.Rating serialize(Rating rating) {
        CommonProtos.Rating.Builder pbb = CommonProtos.Rating.newBuilder();

        pbb.setSource(rating.getPublisher().key());
        pbb.setType(rating.getType());
        pbb.setValue(rating.getValue());

        return pbb.build();
    }

    public Rating deserialize(CommonProtos.Rating ratingPb) {
        return new Rating(
                ratingPb.getType(),
                ratingPb.getValue(),
                Sources.fromPossibleKey(ratingPb.getSource()).get());
    }

}
