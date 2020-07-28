package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.query.Selection;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedContentIndex implements ContentIndex {

    private final ContentIndex delegate;
    private final Timer queryTimer;

    private InstrumentedContentIndex(ContentIndex delegate, MetricRegistry metrics) {
        this.delegate = checkNotNull(delegate);
        this.queryTimer = metrics.timer("EsContentIndex.query");
    }

    public static InstrumentedContentIndex create(ContentIndex delegate, MetricRegistry metrics) {
        return new InstrumentedContentIndex(delegate, metrics);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            Set<AttributeQuery<?>> query,
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
