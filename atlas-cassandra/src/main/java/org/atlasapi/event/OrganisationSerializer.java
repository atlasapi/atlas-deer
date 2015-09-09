package org.atlasapi.event;

import java.util.stream.Collectors;

import org.atlasapi.content.ContentGroupSerializer;
import org.atlasapi.entity.PersonSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class OrganisationSerializer {

    public CommonProtos.Organisation serialize(Organisation organisation) {
        CommonProtos.Organisation.Builder builder = CommonProtos.Organisation.newBuilder();

        builder.setContentGroup(new ContentGroupSerializer<Organisation>().serialize(organisation));

        if (organisation.members() != null) {
            builder.addAllMember(organisation.members().stream()
                    .map(member -> new PersonSerializer().serialize(member))
                    .collect(Collectors.toList()));
        }

        return builder.build();
    }

    public Organisation deserialize(CommonProtos.Organisation msg) {
        Organisation organisation = new Organisation();

        new ContentGroupSerializer<Organisation>().deserialize(msg.getContentGroup(), organisation);

        organisation.setMembers(msg.getMemberList().stream()
                .map(member -> new PersonSerializer().deserialize(member))
                .collect(Collectors.toList()));

        return organisation;
    }
}
