package org.atlasapi.topic;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.topic.EsTopic.ID;
import static org.atlasapi.topic.EsTopic.SOURCE;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.EsObject;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;

public class EsTopicIndex extends AbstractIdleService implements TopicIndex {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(EsTopicIndex.class);
    
    private final Node esClient;
    private final String indexName;
    private final long timeOutDuration;
    private final TimeUnit timeOutUnit;
    
    private final EsQueryBuilder builder = new EsQueryBuilder();

    public EsTopicIndex(Node esClient, String indexName, long timeOutDuration, TimeUnit timeOutUnit) {
        this.esClient = checkNotNull(esClient);
        this.indexName = checkNotNull(indexName);
        this.timeOutDuration = timeOutDuration;
        this.timeOutUnit = checkNotNull(timeOutUnit);
    }

    @Override
    protected void startUp() throws Exception {
        IndicesAdminClient indices = esClient.client().admin().indices();
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
        esClient.client().index(request).actionGet(timeOutDuration, timeOutUnit);
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
        esClient.client()  
            .prepareSearch(indexName)
            .setTypes(EsTopic.TYPE_NAME)
            .setQuery(builder.buildQuery(query))
            .addField(ID)
            .setPostFilter(FiltersBuilder.buildForPublishers(SOURCE, publishers))
            .setFrom(selection.getOffset())
            .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT))
            .addSort(EsTopic.ID, SortOrder.ASC)
            .execute(FutureSettingActionListener.setting(response));
        
        return Futures.transform(response, new Function<SearchResponse, IndexQueryResult>() {
            @Override
            public IndexQueryResult apply(SearchResponse input) {
                /*
                 * TODO: if 
                 *  selection.offset + selection.limit < totalHits
                 * then we have more: return for use with response. 
                 */
                return new IndexQueryResult(FluentIterable.from(input.getHits()).transform(new Function<SearchHit, Id>() {
                    @Override
                    public Id apply(SearchHit hit) {
                        Long id = hit.field(ID).<Number>value().longValue();
                        return Id.valueOf(id);
                    }
                }), input.getHits().getTotalHits());
            }
        });
    }

}
