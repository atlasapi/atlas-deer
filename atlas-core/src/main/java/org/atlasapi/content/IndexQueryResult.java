package org.atlasapi.content;

import java.util.List;

import org.atlasapi.entity.Id;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

public class IndexQueryResult {

    private final ImmutableList<Id> ids;
    private final ImmutableListMultimap<Id, Id> canonicalIdToIdMultiMap;
    private final Long count;

    private IndexQueryResult(Iterable<Id> ids, ImmutableListMultimap<Id, Id> canonicalIdToIdMultiMap,
            long totalResultCount) {
        this.ids = ImmutableList.copyOf(ids);
        this.canonicalIdToIdMultiMap = ImmutableListMultimap.copyOf(canonicalIdToIdMultiMap);
        this.count = totalResultCount;
    }

    public static IndexQueryResult withSingleId(Id id) {
        return new IndexQueryResult(ImmutableList.of(id), ImmutableListMultimap.of(), 1L);
    }

    public static IndexQueryResult withIds(Iterable<Id> ids, long resultCount) {
        return new IndexQueryResult(ids, ImmutableListMultimap.of(), resultCount);
    }

    public static IndexQueryResult withIdsAndCanonicalIds(
            ImmutableListMultimap<Id, Id> canonicalIdToIdMultiMap, long resultCount) {
        return new IndexQueryResult(
                canonicalIdToIdMultiMap.values(),
                canonicalIdToIdMultiMap,
                resultCount
        );
    }

    public Long getTotalCount() {
        return count;
    }

    public FluentIterable<Id> getIds() {
        return FluentIterable.from(ids);
    }

    public List<Id> getIds(Id canonicalId) {
        return ImmutableList.copyOf(canonicalIdToIdMultiMap.get(canonicalId));
    }

    public List<Id> getCanonicalIds() {
        return ImmutableList.copyOf(canonicalIdToIdMultiMap.keySet());
    }
}
