package org.atlasapi.content;

import com.google.common.collect.FluentIterable;
import org.atlasapi.entity.Id;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexQueryResult {

    private final Iterable<Id> ids;
    private final Iterable<Id> canonicalIds;
    private final Long count;

    public IndexQueryResult(Iterable<Id> ids, Iterable<Id> canonicalIds, Long totalResultCount) {
        this.ids = checkNotNull(ids);
        this.canonicalIds = checkNotNull(canonicalIds);
        this.count = checkNotNull(totalResultCount);
    }

    public Long getTotalCount() {
        return count;
    }

    public FluentIterable<Id> getIds() {
        return FluentIterable.from(ids);
    }

    public FluentIterable<Id> getCanonicalIds() {
        return FluentIterable.from(canonicalIds);
    }
}
