package org.atlasapi.system.legacy;

import java.util.stream.Collectors;

import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Organisation;

public class LegacyOrganisationTransformer extends DescribedLegacyResourceTransformer<
        Organisation, org.atlasapi.organisation.Organisation> {

    private final LegacyPersonTransformer legacyPersonTransformer;

    public LegacyOrganisationTransformer() {
        this.legacyPersonTransformer = new LegacyPersonTransformer();
    }

    @Override
    protected org.atlasapi.organisation.Organisation createDescribed(Organisation input) {
        org.atlasapi.organisation.Organisation organisation = new org.atlasapi.organisation.Organisation();

        LegacyContentGroupTransformer.transformInto(organisation, input);

        organisation.setMembers(input.members().stream()
                .map(legacyPersonTransformer::apply)
                .collect(Collectors.toList()));
        organisation.setAlternativeTitles(input.getAlternativeTitles());
        organisation.setCanonicalUri(input.getCanonicalUri());
        organisation.addAlias(new Alias(Alias.URI_NAMESPACE, input.getCanonicalUri()));

        return organisation;
    }

    @Override
    protected Iterable<Alias> moreAliases(Organisation input) {
        return input.getAliases().stream()
                .map(alias -> new Alias(alias.getNamespace(), alias.getValue()))
                .collect(Collectors.toList());
    }
}
