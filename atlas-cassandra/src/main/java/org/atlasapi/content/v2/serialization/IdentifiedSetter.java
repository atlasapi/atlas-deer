package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.v2.model.Identified;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;

import com.google.common.collect.ImmutableSet;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class IdentifiedSetter {

    private final AliasSerialization alias = new AliasSerialization();
    private final EquivalenceRefSerialization equivRef = new EquivalenceRefSerialization();

    public void serialize(Identified internal, org.atlasapi.entity.Identified content) {
        Id id = content.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }

        internal.setCanonicalUri(content.getCanonicalUri());
        internal.setCurie(content.getCurie());
        internal.setAliasUrls(content.getAliasUrls());

        ImmutableSet<Alias> aliases = content.getAliases();
        if (aliases != null) {
            internal.setAliases(aliases.stream()
                    .map(alias::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        Set<EquivalenceRef> equivalentTo = content.getEquivalentTo();
        if (equivalentTo != null) {
            internal.setEquivalentTo(equivalentTo.stream()
                    .map(equivRef::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        internal.setLastUpdated(DateTimeUtils.toInstant(content.getLastUpdated()));
        internal.setEquivalenceUpdate(DateTimeUtils.toInstant(content.getEquivalenceUpdate()));
    }

    public void deserialize(
            org.atlasapi.entity.Identified content, Identified internal) {

        Long id = internal.getId();
        if (id != null) {
            content.setId(id);
        }

        content.setCanonicalUri(internal.getCanonicalUri());
        content.setCurie(internal.getCurie());

        Set<String> aliasUrls = internal.getAliasUrls();
        if (aliasUrls != null) {
            content.setAliasUrls(aliasUrls);
        }

        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            content.setAliases(aliases.stream()
                    .map(alias::deserialize)
                    .collect(Collectors.toSet()));
        }

        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            content.setEquivalentTo(equivalentTo.stream()
                    .map(equivRef::deserialize)
                    .collect(Collectors.toSet()));
        }

        content.setLastUpdated(toDateTime(internal.getLastUpdated()));
        content.setEquivalenceUpdate(toDateTime(internal.getEquivalenceUpdate()));
    }
}