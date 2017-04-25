package org.atlasapi.topic;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.EsObject;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;

import com.metabroadcast.common.query.Selection;

import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.topic.EsTopic.ID;
import static org.atlasapi.topic.EsTopic.SOURCE;

public class EsTopicIndex extends AbstractIdleService implements TopicIndex {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(EsTopicIndex.class);

    private final Client esClient;
    private final String indexName;
    private final long timeOutDuration;
    private final TimeUnit timeOutUnit;

    private final EsQueryBuilder builder = EsQueryBuilder.create();

    public EsTopicIndex(Client esClient, String indexName, long timeOutDuration,
            TimeUnit timeOutUnit) {
        this.esClient = checkNotNull(esClient);
        this.indexName = checkNotNull(indexName);
        this.timeOutDuration = timeOutDuration;
        this.timeOutUnit = checkNotNull(timeOutUnit);
    }

    @Override
    protected void startUp() throws Exception {
        IndicesAdminClient indices = esClient.admin().indices();
        IndicesExistsResponse exists = get(indices.exists(
                Requests.indicesExistsRequest(indexName)
        ));
        if (!exists.isExists()) {
            log.info("Creating index {}", indexName);
            get(indices.create(Requests.createIndexRequest(indexName)));
            get(indices.putMapping(Requests.putMappingRequest(indexName)
                    .type(EsTopic.TYPE_NAME).source(EsTopic.getMapping())
            ));
        } else {
            log.info("Index {} exists", indexName);
        }
    }

    private <T> T get(ActionFuture<T> future) {
        return future.actionGet(timeOutDuration, timeOutUnit);
    }

    @Override
    protected void shutDown() throws Exception {

    }

    public void index(Topic topic) {
        IndexRequest request = Requests.indexRequest(indexName)
                .type(EsTopic.TYPE_NAME)
                .id(topic.getId().toString())
                .source(toEsTopic(topic).toMap());
        esClient.index(request).actionGet(timeOutDuration, timeOutUnit);
        log.debug("indexed {}", topic);
    }

    private EsObject toEsTopic(Topic topic) {
        return new EsTopic()
                .id(topic.getId().longValue())
                .type(topic.getType())
                .source(topic.getSource())
                .aliases(topic.getAliases())
                .title(topic.getTitle())
                .description(topic.getDescription());
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection) {
        SettableFuture<SearchResponse> response = SettableFuture.create();
        esClient
                .prepareSearch(indexName)
                .setTypes(EsTopic.TYPE_NAME)
                .setQuery(builder.buildQuery(query))
                .addField(ID)
                .setPostFilter(FiltersBuilder.buildForPublishers(SOURCE, publishers))
                .setFrom(selection.getOffset())
                .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT))
                .addSort(EsTopic.ID, SortOrder.ASC)
                .execute(FutureSettingActionListener.setting(response));

        return Futures.transform(response, (SearchResponse input) -> {
            /*
             * TODO: if
             *  selection.offset + selection.limit < totalHits
             * then we have more: return for use with response.
             */
            FluentIterable<Id> ids = FluentIterable.from(input.getHits())
                    .transform(hit -> {
                        Long id = hit.field(ID).<Number>value().longValue();
                        return Id.valueOf(id);
                    });
            return IndexQueryResult.withIds(ids, input.getHits().getTotalHits());
        });
    }

}
