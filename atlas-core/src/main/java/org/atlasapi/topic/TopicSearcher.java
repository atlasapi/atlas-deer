package org.atlasapi.topic;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.util.concurrent.ListenableFuture;

public interface TopicSearcher {
    ListenableFuture<IndexQueryResult> query(
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    );
}
