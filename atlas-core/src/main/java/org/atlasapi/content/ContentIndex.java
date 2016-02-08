package org.atlasapi.content;

import java.util.Optional;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.util.concurrent.ListenableFuture;

public interface ContentIndex {

    ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection,
            Optional<IndexQueryParams> searchParam);

    void index(Content content) throws IndexException;

    void updateCanonicalIds(Id canonicalId, Iterable<Id> setIds) throws IndexException;

}
