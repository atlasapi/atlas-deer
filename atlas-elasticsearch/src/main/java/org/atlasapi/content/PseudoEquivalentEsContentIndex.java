package org.atlasapi.content;

import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ImmutableCollectors;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class PseudoEquivalentEsContentIndex implements ContentIndex {

    private final EsContentIndex delegate;

    public PseudoEquivalentEsContentIndex(EsContentIndex delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(AttributeQuerySet query, Iterable<Publisher> publishers, Selection selection, Optional<IndexQueryParams> searchParam) {
        try {

            Selection selectionForDelegate = Selection.limitedTo(
                    selection.limitOrDefaultValue(100) * Iterables.size(publishers)
            );

            IndexQueryResult result = Futures.get(
                    delegate.query(query, publishers, selectionForDelegate, searchParam),
                    Exception.class
            );

            FluentIterable<Id> ids = result.getIds();

            ImmutableList<Id> equivalentResult = ImmutableList.copyOf(ids).stream()
                    .map(Id::longValue)
                    .distinct()
                    .filter(id -> id != null)
                    .limit(selection.limitOrDefaultValue(100))
                    .map(Id::valueOf)
                    .collect(ImmutableCollectors.toList());

            return Futures.immediateFuture(
                    new IndexQueryResult(equivalentResult, result.getTotalCount())
            );
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void index(Content content) throws IndexException {
        delegate.index(content);
    }
}