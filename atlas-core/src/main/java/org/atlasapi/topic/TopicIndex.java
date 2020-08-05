package org.atlasapi.topic;

import org.atlasapi.content.IndexException;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.google.common.util.concurrent.ListenableFuture;

public interface TopicIndex {

    void index(Topic topic) throws IndexException;

    ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection);

}