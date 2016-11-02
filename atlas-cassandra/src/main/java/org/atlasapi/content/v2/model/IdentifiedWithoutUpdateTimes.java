package org.atlasapi.content.v2.model;

import java.util.Set;

import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Ref;

/**
 * @see Identified
 * @see WithUpdateTimes
 */
public interface IdentifiedWithoutUpdateTimes {

    Long getId();

    void setId(Long id);

    String getCanonicalUri();

    void setCanonicalUri(String canonicalUri);

    String getCurie();

    void setCurie(String curie);

    Set<String> getAliasUrls();

    void setAliasUrls(Set<String> aliasUrls);

    Set<Alias> getAliases();

    void setAliases(Set<Alias> aliases);

    Set<Ref> getEquivalentTo();

    void setEquivalentTo(Set<Ref> equivalentTo);
}
