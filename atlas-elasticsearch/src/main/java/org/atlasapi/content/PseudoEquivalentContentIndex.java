package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

public class PseudoEquivalentContentIndex implements ContentIndex {

    private final EsUnequivalentContentIndex delegate;

    public PseudoEquivalentContentIndex(EsUnequivalentContentIndex delegate) {
        this.delegate = checkNotNull(delegate);
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

            return Futures.immediateFuture(
                    IndexQueryResult.withIds(paginatedCanonicalIds, result.getTotalCount())
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
}