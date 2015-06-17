package org.atlasapi.content;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexQueryParams {

    private final Optional<FuzzyQueryParams> fuzzyQueryParams;
    private final Optional<QueryOrdering> ordering;

    public IndexQueryParams(Optional<FuzzyQueryParams> fuzzyQueryParams, Optional<QueryOrdering> ordering) {
        this.fuzzyQueryParams = checkNotNull(fuzzyQueryParams);
        this.ordering = checkNotNull(ordering);
    }

    public Optional<FuzzyQueryParams> getFuzzyQueryParams() {
        return fuzzyQueryParams;
    }

    public Optional<QueryOrdering> getOrdering() {
        return ordering;
    }
}
