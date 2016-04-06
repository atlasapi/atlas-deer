package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;

public class AwardSerializer {

    public CommonProtos.Award serialize(Award award) {
        CommonProtos.Award.Builder awardProtos = CommonProtos.Award.newBuilder();
        awardProtos.setOutcome(award.getOutcome());
        awardProtos.setTitle(award.getTitle());
        awardProtos.setDescription(award.getDescription());
        awardProtos.setYear(award.getYear());
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
