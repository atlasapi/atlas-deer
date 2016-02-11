package org.atlasapi.organisation;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class OrganisationRefSerializer implements Serializer<OrganisationRef, CommonProtos.Reference> {

    public CommonProtos.Reference serialize(OrganisationRef organisationRef) {
        CommonProtos.Reference.Builder builder = CommonProtos.Reference.newBuilder();

        builder.setId(organisationRef.getId().longValue());
        return builder.build();
    }

    public OrganisationRef deserialize(CommonProtos.Reference msg) {

        return new OrganisationRef(Id.valueOf(msg.getId()), "");

    }

}
