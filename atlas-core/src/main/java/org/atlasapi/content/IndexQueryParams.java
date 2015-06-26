package org.atlasapi.content;

import org.atlasapi.entity.Id;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexQueryParams {

    private final Optional<FuzzyQueryParams> fuzzyQueryParams;
    private final Optional<QueryOrdering> ordering;
    private final Optional<Id> regionId;

    public IndexQueryParams(Optional<FuzzyQueryParams> fuzzyQueryParams, Optional<QueryOrdering> ordering, Optional<Id> regionId) {
        this.fuzzyQueryParams = checkNotNull(fuzzyQueryParams);
        this.ordering = checkNotNull(ordering);
        this.regionId = regionId;
    }

    public Optional<FuzzyQueryParams> getFuzzyQueryParams() {
        return fuzzyQueryParams;
    }

    public Optional<QueryOrdering> getOrdering() {
        return ordering;
    }

    public Optional<Id> getRegionId() {
        return regionId;
    }
}
