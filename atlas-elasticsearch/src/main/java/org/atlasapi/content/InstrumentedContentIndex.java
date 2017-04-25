package org.atlasapi.content;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedContentIndex implements ContentIndex {

    private final ContentIndex delegate;
    private final Timer contentIndexTimer;
    private final Timer queryTimer;

    private InstrumentedContentIndex(ContentIndex delegate, MetricRegistry metrics) {
        this.delegate = checkNotNull(delegate);
        this.contentIndexTimer = metrics.timer("EsContentIndex.index");
        this.queryTimer = metrics.timer("EsContentIndex.query");
    }

    public static InstrumentedContentIndex create(ContentIndex delegate, MetricRegistry metrics) {
        return new InstrumentedContentIndex(delegate, metrics);
    }

    @Override
    public void index(Content content) throws IndexException {
        Timer.Context time = contentIndexTimer.time();
        delegate.index(content);
        time.stop();
    }

    @Override
    public void updateCanonicalIds(Id canonicalId, Iterable<Id> setIds) throws IndexException {
        delegate.updateCanonicalIds(canonicalId, setIds);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        Timer.Context time = queryTimer.time();
        ListenableFuture<IndexQueryResult> result = delegate.query(
                query,
                publishers,
                selection
        );
        time.stop();
        return result;
    }
}
