package org.atlasapi.system.legacy;

import java.util.stream.Collectors;

import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Organisation;

public class LegacyOrganisationTransformer extends DescribedLegacyResourceTransformer<
        Organisation, org.atlasapi.event.Organisation> {

    private final LegacyPersonTransformer legacyPersonTransformer;

    public LegacyOrganisationTransformer() {
        this.legacyPersonTransformer = new LegacyPersonTransformer();
    }

    @Override
    protected org.atlasapi.event.Organisation createDescribed(Organisation input) {
        org.atlasapi.event.Organisation organisation = new org.atlasapi.event.Organisation();

        LegacyContentGroupTransformer.transformInto(organisation, input);

        organisation.setMembers(input.members().stream()
                .map(legacyPersonTransformer::apply)
                .collect(Collectors.toList()));

        return organisation;
    }

    @Override
    protected Iterable<Alias> moreAliases(Organisation input) {
        return input.getAliases().stream()
                .map(alias -> new Alias(alias.getNamespace(), alias.getValue()))
                .collect(Collectors.toList());
    }
}
