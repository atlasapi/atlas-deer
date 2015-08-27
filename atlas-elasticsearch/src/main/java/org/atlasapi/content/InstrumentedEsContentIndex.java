package org.atlasapi.content;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.media.entity.Publisher;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedEsContentIndex extends EsContentIndex {

    private final MetricRegistry metrics;
    private final Timer contentGroupIndexTimer;
    private final Timer contentIndexTimer;
    private final Timer queryTimer;

    public InstrumentedEsContentIndex(Client esClient, String indexName, long requestTimeout, ContentResolver resolver, MetricRegistry metrics, ChannelGroupResolver channelGroupResolver) {
        super(esClient, indexName, requestTimeout, resolver, channelGroupResolver);
        this.metrics = checkNotNull(metrics);
        this.contentGroupIndexTimer = metrics.timer("EsContentIndex.index(ContentGroup)");
        this.contentIndexTimer = metrics.timer("EsContentIndex.index(Content)");
        this.queryTimer = metrics.timer("EsContentIndex.query");
    }

    @Override
    public void index(Content content) throws IndexException {
        Timer.Context time = contentIndexTimer.time();
        super.index(content);
        time.stop();
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(AttributeQuerySet query, Iterable<Publisher> publishers, Selection selection, Optional<IndexQueryParams> queryParams) {
        Timer.Context time = queryTimer.time();
        ListenableFuture<IndexQueryResult> result = super.query(query, publishers, selection, queryParams);
        time.stop();
        return result;
    }
}
