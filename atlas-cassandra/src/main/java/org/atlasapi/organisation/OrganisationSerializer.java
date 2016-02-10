package org.atlasapi.organisation;

import java.util.stream.Collectors;

import org.atlasapi.content.ContentGroupSerializer;
import org.atlasapi.entity.PersonSerializer;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class OrganisationSerializer implements Serializer<Organisation, CommonProtos.Organisation> {

    private final ContentGroupSerializer<Organisation> organisationContentGroupSerializer =
            new ContentGroupSerializer<>();
    private final PersonSerializer personSerializer = new PersonSerializer();

    public CommonProtos.Organisation serialize(Organisation organisation) {
        CommonProtos.Organisation.Builder builder = CommonProtos.Organisation.newBuilder();

        builder.setContentGroup(organisationContentGroupSerializer.serialize(organisation));

        if (organisation.members() != null) {
            builder.addAllMember(organisation.members().stream()
                    .map(personSerializer::serialize)
                    .collect(Collectors.toList()));
        }

        if (organisation.getAlternativeTitles() != null) {
            builder.addAllAlternativeTitles(organisation.getAlternativeTitles());
        }

        return builder.build();
    }

    public Organisation deserialize(CommonProtos.Organisation msg) {

        Organisation organisation = new Organisation();

        organisationContentGroupSerializer.deserialize(msg.getContentGroup(), organisation);

        organisation.setMembers(msg.getMemberList().stream()
                .map(personSerializer::deserialize)
                .collect(Collectors.toList()));

        organisation.setAlternativeTitles(msg.getAlternativeTitlesList());

        return organisation;
    }
}
