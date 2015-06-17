package org.atlasapi.content;

import com.google.common.base.Strings;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class FuzzyQueryParams {

    private final String searchTerm;
    private final Optional<Float> boost;

    public FuzzyQueryParams(String searchTerm, Optional<Float> boost) {
        this.searchTerm = checkNotNull(Strings.emptyToNull(searchTerm));
        this.boost = checkNotNull(boost);
    }

    public Optional<Float> getBoost() {
        return boost;
    }

    public String getSearchTerm() {
        return searchTerm;
    }
}
