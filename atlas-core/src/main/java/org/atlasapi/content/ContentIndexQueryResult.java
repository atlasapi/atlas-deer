package org.atlasapi.content;

import com.google.common.collect.FluentIterable;
import org.atlasapi.entity.Id;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentIndexQueryResult {

    private final Long counts;
    private final FluentIterable<Id> ids;

    public ContentIndexQueryResult(Long counts, FluentIterable<Id> ids) {
        this.counts = checkNotNull(counts);
        this.ids = checkNotNull(ids);
    }

    public Long getCounts() {
        return counts;
    }

    public FluentIterable<Id> getFutureIds() {
        return ids;
    }
}
