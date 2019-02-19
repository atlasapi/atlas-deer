package org.atlasapi.content.v2.serialization.setters;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.content.v2.model.IdentifiedWithoutUpdateTimes;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.serialization.AliasSerialization;
import org.atlasapi.content.v2.serialization.EquivalenceRefSerialization;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.equivalence.EquivalenceRef;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class IdentifiedWithoutUpdateTimesSetter {

    private final AliasSerialization alias = new AliasSerialization();
    private final EquivalenceRefSerialization equivRef = new EquivalenceRefSerialization();

    public void serialize(IdentifiedWithoutUpdateTimes internal, Identified identified) {
        Id id = identified.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }

        internal.setCanonicalUri(identified.getCanonicalUri());
        internal.setCurie(identified.getCurie());
        internal.setAliasUrls(identified.getAliasUrls());

        ImmutableSet<Alias> aliases = identified.getAliases();
        if (aliases != null) {
            internal.setAliases(aliases.stream()
                    .map(alias::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        Set<EquivalenceRef> equivalentTo = identified.getEquivalentTo();
        if (equivalentTo != null) {
            internal.setEquivalentTo(equivalentTo.stream()
                    .map(equivRef::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        internal.setCustomFields(identified.getCustomFields());

    }

    public void deserialize(Identified identified, IdentifiedWithoutUpdateTimes internal) {
        Long id = internal.getId();
        if (id != null) {
            identified.setId(id);
        }

        identified.setCanonicalUri(internal.getCanonicalUri());
        identified.setCurie(internal.getCurie());

        Set<String> aliasUrls = internal.getAliasUrls();
        if (aliasUrls != null) {
            identified.setAliasUrls(aliasUrls);
        }

        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            identified.setAliases(aliases.stream()
                    .map(alias::deserialize)
                    .collect(Collectors.toSet()));
        }

        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            identified.setEquivalentTo(equivalentTo.stream()
                    .map(equivRef::deserialize)
                    .collect(Collectors.toSet()));
        }

        Map<String, String> customFields = internal.getCustomFields();
        if (customFields != null) {
            identified.setCustomFields(customFields);
        }
    }
}
