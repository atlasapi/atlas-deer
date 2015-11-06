package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.Id;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

public class IndexQueryResult {

    private final ImmutableSet<Id> ids;
    private final Multimap<Id, Id> canonicalIdToIdMultiMap;
    private final Long count;

    private IndexQueryResult(Iterable<Id> ids, Multimap<Id, Id> canonicalIdToIdMultiMap,
            long totalResultCount) {
        this.ids = ImmutableSet.copyOf(ids);
        this.canonicalIdToIdMultiMap = ImmutableMultimap.copyOf(canonicalIdToIdMultiMap);
        this.count = totalResultCount;
    }

    public static IndexQueryResult withSingleId(Id id) {
        return new IndexQueryResult(ImmutableSet.of(id), ImmutableMultimap.of(), 1L);
    }

    public static IndexQueryResult withIds(Iterable<Id> ids, long resultCount) {
        return new IndexQueryResult(ids, ImmutableMultimap.of(), resultCount);
    }

    public static IndexQueryResult withIdsAndCanonicalIds(Multimap<Id, Id> canonicalIdToIdMultiMap,
            long resultCount) {
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

    public Set<Id> getIds(Id canonicalId) {
        return ImmutableSet.copyOf(canonicalIdToIdMultiMap.get(canonicalId));
    }

    public Set<Id> getCanonicalIds() {
        return canonicalIdToIdMultiMap.keySet();
    }
}
