package org.atlasapi.content;

import com.google.common.collect.FluentIterable;
import org.atlasapi.entity.Id;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexQueryResult {

    private final Iterable<Id> ids;
    private final Long count;

    public IndexQueryResult(Iterable<Id> ids, Long totalResultCount) {
        this.ids = checkNotNull(ids);
        this.count = checkNotNull(totalResultCount);
    }

    public Long getTotalCount() {
        return count;
    }

    public FluentIterable<Id> getIds() {
        return FluentIterable.from(ids);
    }
}
