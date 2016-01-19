package org.atlasapi.content;

import org.atlasapi.entity.Id;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

public class IndexQueryResult {

    private final ImmutableList<Id> ids;
    private final Long count;

    private IndexQueryResult(Iterable<Id> ids, long totalResultCount) {
        this.ids = ImmutableList.copyOf(ids);
        this.count = totalResultCount;
    }

    public static IndexQueryResult withSingleId(Id id) {
        return new IndexQueryResult(ImmutableList.of(id), 1);
    }

    public static IndexQueryResult withIds(Iterable<Id> ids, long resultCount) {
        return new IndexQueryResult(ImmutableList.copyOf(ids), resultCount);
    }

    public Long getTotalCount() {
        return count;
    }

    public FluentIterable<Id> getIds() {
        return FluentIterable.from(ids);
    }
}
