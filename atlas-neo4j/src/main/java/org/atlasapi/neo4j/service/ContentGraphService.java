package org.atlasapi.neo4j.service;

import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Content;
import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.query.GraphQuery;
import org.atlasapi.neo4j.service.query.QueryExecutor;
import org.atlasapi.neo4j.service.writer.ContentWriter;
import org.atlasapi.neo4j.service.writer.GraphWriter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ListenableFuture;
import org.neo4j.ogm.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentGraphService {

    private static final Logger log = LoggerFactory.getLogger(ContentGraphService.class);

    private final ContentWriter contentWriter;
    private final GraphWriter graphWriter;
    private final ContentGraphQueryFactory queryFactory;
    private final QueryExecutor queryExecutor;

    @VisibleForTesting
    ContentGraphService(
            ContentWriter contentWriter,
            GraphWriter graphWriter,
            ContentGraphQueryFactory queryFactory,
            QueryExecutor queryExecutor
    ) {
        this.contentWriter = checkNotNull(contentWriter);
        this.graphWriter = checkNotNull(graphWriter);
        this.queryFactory = checkNotNull(queryFactory);
        this.queryExecutor = checkNotNull(queryExecutor);
    }

    public static ContentGraphService create(Session session) {
        return new ContentGraphService(
                ContentWriter.create(session),
                GraphWriter.create(session),
                ContentGraphQueryFactory.create(),
                QueryExecutor.create(session)
        );
    }

    public void writeEquivalentSet(EquivalenceGraph graph, Iterable<Content> contentInSet) {
        StreamSupport.stream(contentInSet.spliterator(), false)
                .forEach(contentWriter::write);

        graphWriter.writeGraph(graph);
    }

    public Optional<ListenableFuture<IndexQueryResult>> query(IndexQueryParams indexQueryParams,
            Iterable<Publisher> publishers, Map<String, String> parameters) {
        Optional<GraphQuery> graphQuery = queryFactory.getGraphQuery(
                indexQueryParams, publishers, parameters
        );

        if (graphQuery.isPresent()) {
            log.info("Executing query. Query parameters: {}",
                    Joiner.on(",").withKeyValueSeparator("=").join(parameters));
            return Optional.of(queryExecutor.execute(graphQuery.get()));
        }

        log.warn("Requested query not supported. Query parameters: {}",
                Joiner.on(",").withKeyValueSeparator("=").join(parameters));
        return Optional.empty();
    }
}
