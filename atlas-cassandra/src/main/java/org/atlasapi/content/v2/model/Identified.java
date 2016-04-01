package org.atlasapi.content.v2.model;

import java.util.Set;

import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Ref;

import org.joda.time.Instant;

/**
 * Created by emils on 04/04/2016.
 */
public interface Identified {

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

    Instant getLastUpdated();

    void setLastUpdated(Instant lastUpdated);

    Instant getEquivalenceUpdate();

    void setEquivalenceUpdate(Instant equivalenceUpdate);
}
