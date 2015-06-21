package org.atlasapi.content;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.node.Node;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedEsContentIndex extends EsContentIndex {

    private final MetricRegistry metrics;
    private final Timer contentGroupIndexTimer;
    private final Timer contentIndexTimer;
    private final Timer queryTimer;

    public InstrumentedEsContentIndex(Node esClient, String indexName, long requestTimeout, ContentResolver resolver, MetricRegistry metrics) {
        super(esClient, indexName, requestTimeout, resolver);
        this.metrics = checkNotNull(metrics);
        this.contentGroupIndexTimer = metrics.timer("EsContentIndex.index(ContentGroup)");
        this.contentIndexTimer = metrics.timer("EsContentIndex.index(Content)");
        this.queryTimer = metrics.timer("EsContentIndex.query");
    }

    @Override
    public void index(ContentGroup cg) throws IndexException {
        Timer.Context time = contentGroupIndexTimer.time();
        super.index(cg);
        time.stop();
    }

    @Override
    public void index(Content content) throws IndexException {
        Timer.Context time = contentIndexTimer.time();
        super.index(content);
        time.stop();
    }

    @Override
    public ListenableFuture<FluentIterable<Id>> query(AttributeQuerySet query, Iterable<Publisher> publishers, Selection selection, Optional<IndexQueryParams> queryParams) {
        Timer.Context time = queryTimer.time();
        ListenableFuture<FluentIterable<Id>> result = super.query(query, publishers, selection, queryParams);
        time.stop();
        return result;
    }
}
