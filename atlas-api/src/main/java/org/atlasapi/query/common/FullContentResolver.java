package org.atlasapi.query.common;

import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.topic.Topic;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FullContentResolver {

    public QueryResult<ResolvedContent> resolve(QueryResult<Content> queryResult) {
        if (queryResult.isListResult()) {
            return QueryResult.listResult(
                    resolveContent(queryResult.getResources(), queryResult.getContext()),
                    queryResult.getContext(),
                    queryResult.getTotalResults()
            );
        } else {
            return QueryResult.singleResult(
                    resolveContent(queryResult.getOnlyResource(), queryResult.getContext()),
                    queryResult.getContext()
            );
        }
    }

    public ContextualQueryResult<Topic, ResolvedContent> resolve(
            ContextualQueryResult<Topic, Content> queryResult
    ) {
        return ContextualQueryResult.valueOf(
                queryResult.getContextResult(),
                resolveListQuery(queryResult.getResourceResult()),
                queryResult.getContext()
        );
    }

    private QueryResult.ListQueryResult<ResolvedContent> resolveListQuery(
            QueryResult.ListQueryResult<Content> listQuery
    ) {
        return QueryResult.ListQueryResult.listResult(
                resolveContent(listQuery.getResources(), listQuery.getContext()),
                listQuery.getContext(),
                listQuery.getTotalResults()
        );
    }

    private ResolvedContent resolveContent(Content content, QueryContext context) {
        return null;
    }

    private Iterable<ResolvedContent> resolveContent(Iterable<Content> contents, QueryContext context) {
        return StreamSupport.stream(contents.spliterator(), false)
                .map(content -> resolveContent(content, context))
                .collect(MoreCollectors.toImmutableList());
    }

}
