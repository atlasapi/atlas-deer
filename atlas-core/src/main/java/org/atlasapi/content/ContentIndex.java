package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.util.concurrent.ListenableFuture;

public interface ContentIndex {
    ListenableFuture<IndexQueryResult> query(
            Set<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    );
}
