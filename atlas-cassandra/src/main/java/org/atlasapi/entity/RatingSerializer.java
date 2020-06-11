package org.atlasapi.entity;

import java.util.Optional;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.source.Sources;

public class RatingSerializer {
    public CommonProtos.Rating serialize(Rating rating) {
        CommonProtos.Rating.Builder ratingBuilder = CommonProtos.Rating.newBuilder();

        ratingBuilder.setSource(rating.getPublisher().key());
        ratingBuilder.setType(rating.getType());
        ratingBuilder.setValue(rating.getValue());
        if(rating.getNumberOfVotes() != null) {
            ratingBuilder.setNumberOfVotes(rating.getNumberOfVotes());
        }

        return ratingBuilder.build();
    }

    public Optional<Rating> deserialize(CommonProtos.Rating ratingBuffer) {
        // all the fields of this protocol buffer are optional for future
        // compatibility.  This incarnation requires all fields to be present
        if (ratingBuffer.hasType() && ratingBuffer.hasValue() && ratingBuffer.hasSource()) {
            return Optional.of(new Rating(
                            ratingBuffer.getType(),
                            ratingBuffer.getValue(),
                            Sources.fromPossibleKey(ratingBuffer.getSource()).get(),
                            ratingBuffer.hasNumberOfVotes() ? ratingBuffer.getNumberOfVotes() : null
                    )
            );
        }

        return Optional.empty();
    }
}
