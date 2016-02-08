package org.atlasapi.equivalence;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.collect.OptionalMap;

public interface EquivalenceSummaryStore {

    void store(EquivalenceSummary summary);

    OptionalMap<Id, EquivalenceSummary> summariesForIds(Iterable<Id> ids);

}
