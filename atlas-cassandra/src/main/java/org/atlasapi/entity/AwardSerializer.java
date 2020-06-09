package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;

public class AwardSerializer {

    public CommonProtos.Award serialize(Award award) {
        CommonProtos.Award.Builder awardProtos = CommonProtos.Award.newBuilder();

        //TODO: this might need some better handling, i.e. if there is not enough information
        //to construct the award, probably discard the whole Award by returning null and log the
        //the error (as opposed to not letting the content being created).
        awardProtos.setTitle(award.getTitle());

        if (award.getOutcome() != null) {
            awardProtos.setOutcome(award.getOutcome());
        }
        if (award.getDescription() != null) {
            awardProtos.setDescription(award.getDescription());
        }
        if (award.getYear() != null) {
            awardProtos.setYear(award.getYear());
        }
        return awardProtos.build();
    }

    public Award deserialize(CommonProtos.Award awardProtos) {
        Award award = new Award();
        award.setOutcome(awardProtos.getOutcome());
        award.setTitle(awardProtos.getTitle());
        award.setDescription(awardProtos.getDescription());
        award.setYear(awardProtos.getYear());
        return award;
    }
}
