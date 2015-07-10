package org.atlasapi.content;

import org.atlasapi.entity.Id;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An ID to be included or excluded from a query
 */
public class InclusionExclusionId {

    private final Id id;
    private final Boolean included;

    public InclusionExclusionId(Id id, Boolean included) {
        this.id = checkNotNull(id);
        this.included = checkNotNull(included);
    }

    public Id getId() {
        return id;
    }

    public Boolean isIncluded() {
        return included;
    }

    public Boolean isExcluded() {
        return !included;
    }

    public static InclusionExclusionId valueOf(Id id, Boolean included) {
        return new InclusionExclusionId(id, included);
    }
}
