package org.atlasapi.system.health.probes;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResults;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchProbe extends Probe {

    private final EsContentTitleSearcher esContentTitleSearcher;

    private ElasticsearchProbe(String identifier, EsContentTitleSearcher esContentTitleSearcher) {
        super(identifier);
        this.esContentTitleSearcher = checkNotNull(esContentTitleSearcher);
    }

    public static ElasticsearchProbe create(
            String identifier,
            EsContentTitleSearcher esContentTitleSearcher
    ) {
        return new ElasticsearchProbe(identifier, esContentTitleSearcher);
    }

    @Override
    public Callable<ProbeResult> createRequest() {
        return () -> {
            SearchQuery query = SearchQuery.builder("eastenders")
                    .withSelection(Selection.offsetBy(0))
                    .withSpecializations(ImmutableSet.of())
                    .withPublishers(ImmutableSet.of())
                    .withTitleWeighting(1)
                    .withBroadcastWeighting(0)
                    .withCatchupWeighting(0)
                    .withPriorityChannelWeighting(0)
                    .build();

            ListenableFuture<SearchResults> futureResults = esContentTitleSearcher.search(query);

            try {
                futureResults.get(25, TimeUnit.SECONDS);
                return ProbeResult.healthy(identifier);

            } catch (InterruptedException | ExecutionException e) {
                return ProbeResult.unhealthy(identifier, e);
            }


        };
    }

}
