package org.atlasapi.content;

import java.util.Optional;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This interface is used when a content index is used as a delegate of another content index to
 * pass additional information between them without polluting the {@link ContentIndex} interface
 * with it.
 */
public interface DelegateContentIndex {

    ListenableFuture<DelegateIndexQueryResult> delegateQuery(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection,
            Optional<IndexQueryParams> queryParams);
}
