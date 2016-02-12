package org.atlasapi.organisation;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.source.Sources;

public class OrganisationRefSerializer implements Serializer<OrganisationRef, CommonProtos.Reference> {

    public CommonProtos.Reference serialize(OrganisationRef organisationRef) {
        CommonProtos.Reference.Builder builder = CommonProtos.Reference.newBuilder();

        builder.setId(organisationRef.getId().longValue());
        builder.setSource(organisationRef.getSource().key());
        return builder.build();
    }

    public OrganisationRef deserialize(CommonProtos.Reference msg) {

        return new OrganisationRef(Id.valueOf(msg.getId()), Sources.fromPossibleKey(msg.getSource()).orNull());

    }

}
