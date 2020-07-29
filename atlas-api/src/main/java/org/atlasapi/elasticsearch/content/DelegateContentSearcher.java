package org.atlasapi.elasticsearch.content;

import java.util.Set;

import org.atlasapi.content.ContentSearcher;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This interface is used when a content index is used as a delegate of another content index to
 * pass additional information between them without polluting the {@link ContentSearcher} interface
 * with it.
 */
public interface DelegateContentSearcher {
    ListenableFuture<DelegateIndexQueryResult> delegateQuery(
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    );
}
