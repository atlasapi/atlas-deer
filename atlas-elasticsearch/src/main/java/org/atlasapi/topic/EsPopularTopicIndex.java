package org.atlasapi.topic;

import org.atlasapi.EsSchema;
import org.atlasapi.content.EsBroadcast;
import org.atlasapi.content.EsContent;
import org.atlasapi.content.EsTopicMapping;
import org.atlasapi.entity.Id;
import org.atlasapi.util.FutureSettingActionListener;

import com.metabroadcast.common.query.Selection;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsPopularTopicIndex implements PopularTopicIndex {

    private static final String TOPIC_FACET_NAME = EsContent.TOPICS;
    private static final String TOPIC_ID_FIELD = EsContent.TOPICS + "." + EsTopicMapping.TOPIC_ID;

    private final Client index;

    public EsPopularTopicIndex(Client index) {
        this.index = checkNotNull(index);
    }

    @Override
    public ListenableFuture<FluentIterable<Id>> popularTopics(Interval interval,
            final Selection selection) {
        SettableFuture<SearchResponse> response = SettableFuture.create();
        prepareQuery(interval, selection)
                .execute(FutureSettingActionListener.setting(response));
        return Futures.transform(response, new Function<SearchResponse, FluentIterable<Id>>() {

            @Override
            public FluentIterable<Id> apply(SearchResponse input) {
                Facets facets = input.getFacets();
                TermsFacet terms = facets.facet(TermsFacet.class, TOPIC_FACET_NAME);
                return FluentIterable.from(terms)
                        .skip(selection.getOffset())
                        .transform(new Function<Entry, Id>() {

                            @Override
                            public Id apply(Entry input) {
                                return Id.valueOf(input.getTerm().string());
                            }
                        });
            }
        });
    }

    private SearchRequestBuilder prepareQuery(Interval interval, Selection selection) {
        return index.prepareSearch(EsSchema.CONTENT_INDEX)
                .setQuery(QueryBuilders.nestedQuery(
                        EsContent.BROADCASTS,
                        QueryBuilders.rangeQuery(EsBroadcast.TRANSMISSION_TIME)
                                .from(interval.getStart())
                                .to(interval.getEnd())
                ))
                .addFacet(FacetBuilders.termsFacet(TOPIC_FACET_NAME)
                        .nested(EsContent.TOPICS + "." + EsTopicMapping.TOPIC)
                        .field(TOPIC_ID_FIELD)
                        .size(selection.getOffset() + selection.getLimit())
                );
    }

}
