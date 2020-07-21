package org.atlasapi.query.v4.content;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexBackedEquivalentContentQueryExecutor implements QueryExecutor<Content> {

    private final ContentIndex index;
    private final MergingEquivalentsResolver<Content> resolver;
    private static final Logger log = LoggerFactory.getLogger(IndexBackedEquivalentContentQueryExecutor.class);

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
    public QueryResult<Content> execute(Query<Content> query) throws QueryExecutionException {
        try {
            return Futures.getChecked(
                    executeQuery(query),
                    QueryExecutionException.class,
                    1,
                    TimeUnit.MINUTES
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
        } catch (NullPointerException npe) {
            // this shouldn't really ever happen I think
            log.error("There is no future to get for query with ID {}", query.getOnlyId().toString(), npe);
            throw Throwables.propagate(npe);
        }
    }

    private ListenableFuture<QueryResult<Content>> executeQuery(Query<Content> query)
            throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private ListenableFuture<QueryResult<Content>> executeSingleQuery(Query<Content> query) {
        final Id contentId = query.getOnlyId();
        return Futures.transform(
                resolve(query, contentId),
                (ResolvedEquivalents<Content> input) -> {
                    log.info("Executing single query for contentId {}", contentId);
                    if (input == null) {
                        log.error("No content for the contentId {}", contentId);
                        throw new UncheckedQueryExecutionException(
                                new NotFoundException(contentId)
                        );
                    }
                    List<Content> equivs = input.get(contentId).asList();
                    if (equivs.isEmpty()) {
                        log.error("No resolved equivalents for the contentId {}", contentId);
                        throw new UncheckedQueryExecutionException(
                                new NotFoundException(contentId)
                        );
                    }
                    Content resource = equivs.get(0);
                    return QueryResult.singleResult(resource, query.getContext());
                }
        );
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolve(Query<Content> query, Id id) {
        return resolver.resolveIds(
                ImmutableSet.of(id),
                application(query),
                annotations(query),
                operands(query)
        );
    }

    private ListenableFuture<QueryResult<Content>> executeListQuery(Query<Content> query)
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

    private ListenableFuture<IndexQueryResult> executeIndexQuery(Query<Content> query) {
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

    private ListenableFuture<QueryResult<Content>> resolveSearchQuery(
            Query<Content> query,
            IndexQueryResult input
    ) {
        ListenableFuture<ResolvedEquivalents<Content>> resolving =
                resolver.resolveIds(
                        input.getIds(),
                        application(query),
                        annotations(query),
                        operands(query)
                );
        return Futures.transform(
                resolving,
                (ResolvedEquivalents<Content> resolved) -> {
                    Iterable<Content> resources = resolved.getFirstElems();
                    return QueryResult.listResult(
                            resources, query.getContext(),
                            input.getTotalCount()
                    );
                });
    }

    private Selection selection(Query<Content> query) {
        return query.getContext().getSelection().or(Selection.all());
    }

    private ImmutableSet<Publisher> sources(Query<Content> query) {
        return application(query).getConfiguration().getEnabledReadSources();
    }

    private Set<Annotation> annotations(Query<Content> query) {
        return ImmutableSet.copyOf(query.getContext().getAnnotations().values());
    }

    private Application application(Query<Content> query) {
        return query.getContext().getApplication();
    }

    private AttributeQuerySet operands(Query<Content> query) {
        return query.getOperands();
    }
}
