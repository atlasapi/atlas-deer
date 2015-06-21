package org.atlasapi.query.v4.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Ordering;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.IndexQueryParser;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.metabroadcast.common.query.Selection;

import javax.annotation.Nullable;

public class IndexBackedEquivalentContentQueryExecutor implements QueryExecutor<Content> {

    private final ContentIndex index;
    private final MergingEquivalentsResolver<Content> resolver;
    private final IndexQueryParser indexQueryParser = new IndexQueryParser();

    public IndexBackedEquivalentContentQueryExecutor(ContentIndex contentIndex,
            MergingEquivalentsResolver<Content> equivalentContentResolver) {
        this.index = checkNotNull(contentIndex);
        this.resolver = checkNotNull(equivalentContentResolver);
    }

    @Override
    public QueryResult<Content> execute(Query<Content> query) throws QueryExecutionException {
        try {
            return Futures.get(executeQuery(query), 1, TimeUnit.MINUTES, QueryExecutionException.class);
        } catch (UncheckedExecutionException | UncheckedQueryExecutionException ee) {
            Throwables.propagateIfInstanceOf(Throwables.getRootCause(ee), QueryExecutionException.class);
            throw Throwables.propagate(ee);
        }
    }

    private ListenableFuture<QueryResult<Content>> executeQuery(Query<Content> query)
            throws QueryExecutionException {
            return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private ListenableFuture<QueryResult<Content>> executeSingleQuery(final Query<Content> query) {
        final Id contentId = query.getOnlyId();
        return Futures.transform(resolve(query, contentId),
                (ResolvedEquivalents<Content> input) -> {
                    List<Content> equivs = input.get(contentId).asList();
                    if (equivs.isEmpty()) {
                        throw new UncheckedQueryExecutionException(new NotFoundException(contentId));
                    }
                    Content resource = equivs.get(0);
                    return QueryResult.singleResult(resource, query.getContext());
                }
        );
    }

    private ListenableFuture<ResolvedEquivalents<Content>> resolve(Query<Content> query, Id id) {
        return resolver.resolveIds(ImmutableSet.of(id), applicationSources(query), annotations(query));
    }

    private ListenableFuture<QueryResult<Content>> executeListQuery(final Query<Content> query) throws QueryExecutionException {
        try {
            ListenableFuture<FluentIterable<Id>> hits
                    = index.query(query.getOperands(), sources(query), selection(query), Optional.of(indexQueryParser.parse(query)));
            return Futures.transform(Futures.transform(hits, toEquivalentContent(query)), toQueryResult(query));
        } catch (QueryParseException qpe) {
            throw new QueryExecutionException(qpe);
        }
    }

    private AsyncFunction<FluentIterable<Id>, Iterable<Content>> toEquivalentContent(
            final Query<Content> query) {
        return input -> {
            Ordering<Id> idOrdering = Ordering.explicit(input.toList());
            return Futures.transform(
                    resolver.resolveIds(input, applicationSources(query), annotations(query)),
                    new Function<ResolvedEquivalents<Content>, Iterable<Content>>() {
                        @Nullable
                        @Override
                        public Iterable<Content> apply(ResolvedEquivalents<Content> input) {
                            return StreamSupport.stream(input.getFirstElems().spliterator(), false)
                                    .sorted((o1, o2) -> idOrdering.compare(o1.getId(), o2.getId()))
                                    .collect(Collectors.toList());
                        }
                    }
            );
        };
    }

    private Function<Iterable<Content>, QueryResult<Content>> toQueryResult(final Query<Content> query) {
        return input -> QueryResult.listResult(input, query.getContext());
    }

    private Selection selection(Query<Content> query) {
        return query.getContext().getSelection().or(Selection.all());
    }

    private ImmutableSet<Publisher> sources(Query<Content> query) {
        return applicationSources(query).getEnabledReadSources();
    }
    
    private Set<Annotation> annotations(Query<Content> query) {
        return ImmutableSet.copyOf(query.getContext().getAnnotations().values());
    }
    
    private ApplicationSources applicationSources(Query<Content> query) {
        return query.getContext().getApplicationSources();
    }

}
