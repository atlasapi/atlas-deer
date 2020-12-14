package org.atlasapi.elasticsearch.content;

import org.atlasapi.content.ContentSearcher;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedContentSearcher implements ContentSearcher {

    private final ContentSearcher delegate;
    private final Timer queryTimer;

    private InstrumentedContentSearcher(ContentSearcher delegate, MetricRegistry metrics) {
        this.delegate = checkNotNull(delegate);
        this.queryTimer = metrics.timer("EsContentIndex.query");
    }

    public static InstrumentedContentSearcher create(ContentSearcher delegate, MetricRegistry metrics) {
        return new InstrumentedContentSearcher(delegate, metrics);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            Iterable<AttributeQuery<?>> query,
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
