package org.atlasapi.query.v4.content;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.exceptions.UncheckedQueryExecutionException;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.promise.Promise;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexBackedEquivalentContentQueryExecutor implements QueryExecutor<ResolvedContent> {

    private final ContentIndex index;
    private final MergingEquivalentsResolver<Content> resolver;

    private IndexBackedEquivalentContentQueryExecutor(
            ContentIndex contentIndex,
            MergingEquivalentsResolver<Content> equivalentContentResolver
    ) {
        this.index = checkNotNull(contentIndex);
        this.resolver = checkNotNull(equivalentContentResolver);
    }

    public static IndexBackedEquivalentContentQueryExecutor create(
            ContentIndex contentIndex,
            MergingEquivalentsResolver<Content> equivalentContentResolver
    ) {
        return new IndexBackedEquivalentContentQueryExecutor(
                contentIndex,
                equivalentContentResolver
        );
    }

    @Override
    public QueryResult<ResolvedContent> execute(Query<ResolvedContent> query) throws QueryExecutionException {
        try {
            return Futures.get(
                    executeQuery(query),
                    1,
                    TimeUnit.MINUTES,
                    QueryExecutionException.class
            );
        } catch (UncheckedExecutionException | UncheckedQueryExecutionException ee) {
            for (Throwable throwable : Throwables.getCausalChain(ee)) {
                if (throwable instanceof NotFoundException) {
                    throw (NotFoundException) throwable;
                }
            }
            Throwables.propagateIfInstanceOf(
                    Throwables.getRootCause(ee),
                    QueryExecutionException.class
            );
            throw Throwables.propagate(ee);
        }
    }

    private ListenableFuture<QueryResult<ResolvedContent>> executeQuery(Query<ResolvedContent> query)
            throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private ListenableFuture<QueryResult<ResolvedContent>> executeSingleQuery(Query<ResolvedContent> query) {
        final Id contentId = query.getOnlyId();
        return Futures.transform(
                resolve(query, contentId),
                (ResolvedEquivalents<Content> input) -> {
                    List<Content> equivs = input.get(contentId).asList();
                    if (equivs.isEmpty()) {
                        throw new UncheckedQueryExecutionException(
                                new NotFoundException(contentId)
                        );
                    }
                    Content resource = equivs.get(0);
                    return QueryResult.singleResult(
                            ResolvedContent.resolvedContentBuilder().withContent(resource).build(),
                            query.getContext()
                    );
                }
        );
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolve(Query<ResolvedContent> query, Id id) {
        return resolver.resolveIds(
                ImmutableSet.of(id),
                application(query),
                annotations(query)
        );
    }

    private ListenableFuture<QueryResult<ResolvedContent>> executeListQuery(Query<ResolvedContent> query)
            throws QueryExecutionException {
        try {
            ListenableFuture<IndexQueryResult> result = executeIndexQuery(query);

            return Promise.wrap(result)
                    .then(queryResult -> resolveSearchQuery(query, queryResult))
                    .get();
        } catch (Exception e) {
            throw new QueryExecutionException(e);
        }
    }

    private ListenableFuture<IndexQueryResult> executeIndexQuery(Query<ResolvedContent> query) {
        // Check if the query is requesting specific IDs
        Optional<ListenableFuture<IndexQueryResult>> naiveResult = query.getOperands()
                .stream()
                .filter(attributeQuery -> attributeQuery.getAttribute().equals(Attributes.ID))
                .map(attributeQuery -> (IdAttributeQuery) attributeQuery)
                .map(AttributeQuery::getValue)
                .map(ids -> IndexQueryResult.withIds(ids, ids.size()))
                .map(Futures::immediateFuture)
                .findFirst();

        if (naiveResult.isPresent()) {
            return naiveResult.get();
        }

        return index.query(
                query.getOperands(),
                sources(query),
                selection(query)
        );
    }

    private ListenableFuture<QueryResult<ResolvedContent>> resolveSearchQuery(
            Query<ResolvedContent> query,
            IndexQueryResult input
    ) {
        ListenableFuture<ResolvedEquivalents<Content>> resolving =
                resolver.resolveIds(
                        input.getIds(),
                        application(query),
                        annotations(query)
                );
        return Futures.transform(
                resolving,
                (ResolvedEquivalents<Content> resolved) -> {
                    Iterable<ResolvedContent> resources = StreamSupport.stream(
                            resolved.getFirstElems().spliterator(),
                            false
                    )
                            .map(content -> ResolvedContent.resolvedContentBuilder().withContent(content).build())
                            .collect(Collectors.toList());
                    return QueryResult.listResult(
                            resources, query.getContext(),
                            input.getTotalCount()
                    );
                });
    }

    private Selection selection(Query<ResolvedContent> query) {
        return query.getContext().getSelection().or(Selection.all());
    }

    private ImmutableSet<Publisher> sources(Query<ResolvedContent> query) {
        return application(query).getConfiguration().getEnabledReadSources();
    }

    private Set<Annotation> annotations(Query<ResolvedContent> query) {
        return ImmutableSet.copyOf(query.getContext().getAnnotations().values());
    }

    private Application application(Query<ResolvedContent> query) {
        return query.getContext().getApplication();
    }
}
