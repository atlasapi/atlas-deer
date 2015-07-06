package org.atlasapi.content;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.media.entity.Publisher;

import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

import java.util.Optional;

public interface ContentIndex {

    ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection, Optional<IndexQueryParams> searchParam);
    
    void index(Content content) throws IndexException;

    void index(ContentGroup contentGroup) throws IndexException;
    
}
