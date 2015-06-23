package org.atlasapi.query.v4.topic;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.ContextualQuery;
import org.atlasapi.query.common.ContextualQueryExecutor;
import org.atlasapi.query.common.ContextualQueryResult;
import org.atlasapi.query.common.ForbiddenException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;

public class TopicContentQueryExecutor implements ContextualQueryExecutor<Topic, Content> {

    private static final long QUERY_TIMEOUT = 60000;

    private final TopicResolver topicResolver;
    private final ContentIndex index;
    private final MergingEquivalentsResolver<Content> contentResolver;

    public TopicContentQueryExecutor(TopicResolver topicResolver, ContentIndex index,
        MergingEquivalentsResolver<Content> equivalentsResolver) {
        this.topicResolver = checkNotNull(topicResolver);
        this.index = checkNotNull(index);
        this.contentResolver = checkNotNull(equivalentsResolver);
    }

    @Override
    public ContextualQueryResult<Topic, Content> execute(final ContextualQuery<Topic, Content> query)
        throws QueryExecutionException {
        return Futures.get(Futures.transform(
                resolveTopic(query.getContextQuery().getOnlyId()), resolveContentToContextualQuery(query)
            ), QUERY_TIMEOUT, MILLISECONDS, QueryExecutionException.class);
    }

    private AsyncFunction<Resolved<Topic>, ContextualQueryResult<Topic, Content>> resolveContentToContextualQuery(
            final ContextualQuery<Topic, Content> query) {
        return resolved -> {
                com.google.common.base.Optional<Topic> possibleTopic = resolved.getResources().first();

                if (!possibleTopic.isPresent()) {
                    throw new NotFoundException(query.getContextQuery().getOnlyId());
                }

                final Topic topic = possibleTopic.get();

                final QueryContext context = query.getContext();
                if (!context.getApplicationSources().isReadEnabled(topic.getSource())) {
                    throw new ForbiddenException(topic.getId());
                }

                return Futures.transform(resolveContent(queryIndex(query.getResourceQuery()), query.getContext()),
                        toContextualQuery(topic, context));
            };
    }
    
    private Function<ResolvedEquivalents<Content>, ContextualQueryResult<Topic, Content>> toContextualQuery(
            final Topic topic, final QueryContext context) {
        return content -> ContextualQueryResult.valueOf(
                QueryResult.singleResult(topic, context),
                QueryResult.listResult(content.getFirstElems(), context, Long.valueOf(content.size())),
                context);
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolveContent(
            ListenableFuture<IndexQueryResult> queryIndex, QueryContext context) {
        return resolveContent(queryIndex, context.getApplicationSources(), context.getAnnotations().all());
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolveContent(
            ListenableFuture<IndexQueryResult> queryHits,
            final ApplicationSources sources, final Set<Annotation> annotations) {
        return Futures.transform(queryHits,
                (IndexQueryResult ids) -> {
                    return contentResolver.resolveIds(ids.getIds(), sources, annotations);
                });
    }
    
    private ListenableFuture<Resolved<Topic>> resolveTopic(Id contextId) {
        return topicResolver.resolveIds(ImmutableList.of(contextId));
    }
    
    private ListenableFuture<IndexQueryResult> queryIndex(Query<Content> query) throws QueryExecutionException {
        return index.query(
            query.getOperands(), 
            query.getContext().getApplicationSources().getEnabledReadSources(), 
            query.getContext().getSelection().or(Selection.all()),
                Optional.empty());
    }

}
