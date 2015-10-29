package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

public class PseudoEquivalentContentIndex implements ContentIndex {

    private static final Logger LOG = LoggerFactory.getLogger(PseudoEquivalentContentIndex.class);

    private final EsUnequivalentContentIndex delegate;
    private final SecondaryIndex equivIdIndex;

    public PseudoEquivalentContentIndex(EsUnequivalentContentIndex delegate,
            SecondaryIndex equivIdIndex) {
        this.delegate = checkNotNull(delegate);
        this.equivIdIndex = checkNotNull(equivIdIndex);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection,
            Optional<IndexQueryParams> searchParam) {
        try {

            Selection selectionForDelegate = getSelectionForDelegate(publishers, selection);

            IndexQueryResult result = Futures.get(
                    delegate.query(query, publishers, selectionForDelegate, searchParam),
                    Exception.class
            );

            Set<Id> canonicalIds = result.getCanonicalIds();
            ImmutableList<Id> paginatedCanonicalIds = paginateIds(canonicalIds, selection);

            ImmutableList<Id> ids = resolveCanonicalIds(paginatedCanonicalIds, result);

            return Futures.immediateFuture(
                    IndexQueryResult.withIds(ids, result.getTotalCount())
            );

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void index(Content content) throws IndexException {
        delegate.index(content);
    }

    @Override
    public void updateCanonicalIds(Id canonicalId, Iterable<Id> setIds) throws IndexException {
        delegate.updateCanonicalIds(canonicalId, setIds);
    }

    private Selection getSelectionForDelegate(Iterable<Publisher> publishers, Selection selection) {
        int numberOfSources = Iterables.size(publishers);
        int delegateLimit = selection.limitOrDefaultValue(100) * numberOfSources;

        if(selection.hasNonZeroOffset()) {
            delegateLimit += selection.getOffset() * numberOfSources;
        }

        return Selection.limitedTo(delegateLimit);
    }

    private ImmutableList<Id> paginateIds(Iterable<Id> ids, Selection selection) {
         return StreamSupport.stream(ids.spliterator(), false)
                .skip(selection.hasNonZeroOffset() ? selection.getOffset() : 0)
                .limit(selection.limitOrDefaultValue(100))
                .collect(ImmutableCollectors.toList());
    }

    // This is to deal with a bug where the canonical ID in the index is wrong
    // To fix it we are trying to resolve the canonical ID from the secondary index
    // using the IDs in the index canonical ID. This still returns the index canonical
    // ID if it fails to resolve it from the secondary index
    private ImmutableList<Id> resolveCanonicalIds(Iterable<Id> indexCanonicalIds,
            IndexQueryResult queryResult) {
        return StreamSupport.stream(indexCanonicalIds.spliterator(), false)
                .map(indexCanonicalId -> resolveCanonicalIdFromIndexCanonicalId(
                        indexCanonicalId, queryResult
                ))
                .collect(ImmutableCollectors.toList());
    }

    private Id resolveCanonicalIdFromIndexCanonicalId(Id indexCanonicalId,
            IndexQueryResult queryResult) {

        for (Id id : queryResult.getIds(indexCanonicalId)) {
            Optional<Id> resolvedCanonicalId = resolveCanonicalId(id);

            if(resolvedCanonicalId.isPresent()) {
                return resolvedCanonicalId.get();
            }
        }

        LOG.warn("Failed to resolve canonical ID for index canonical ID {}", indexCanonicalId);
        return indexCanonicalId;
    }

    private Optional<Id> resolveCanonicalId(Id id) {
        try {
            ListenableFuture<ImmutableMap<Long, Long>> result =
                    equivIdIndex.lookup(ImmutableList.of(id.longValue()));
            ImmutableMap<Long, Long> idToCanonical = Futures.get(result, IOException.class);
            if(idToCanonical.get(Long.valueOf(id.longValue())) != null) {
                return Optional.of(Id.valueOf(idToCanonical.get(id.longValue())));
            }
            LOG.warn("Found no canonical ID for {} using {}", id, id);
        } catch (IOException e) {
            LOG.warn("Found no canonical ID for {} using {}", id, id, e);
        }
        return Optional.empty();
    }
}