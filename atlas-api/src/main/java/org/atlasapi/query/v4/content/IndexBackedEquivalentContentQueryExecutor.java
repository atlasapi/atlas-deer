package org.atlasapi.query.v4.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.QueryOrdering;
import org.atlasapi.content.TitleQueryParams;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
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

public class IndexBackedEquivalentContentQueryExecutor implements QueryExecutor<Content> {

    private final ContentIndex index;
    private final MergingEquivalentsResolver<Content> resolver;

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
        ListenableFuture<FluentIterable<Id>> hits
            = index.query(query.getOperands(), sources(query), selection(query), orderingFrom(query), titleQueryFrom(query));
        return Futures.transform(Futures.transform(hits, toEquivalentContent(query)), toQueryResult(query));
    }

    private Optional<QueryOrdering> orderingFrom(Query<Content> query) {
        Map params = query.getContext().getRequest().getParameterMap();
        if (!params.containsKey("order_by")) {
            return Optional.empty();
        }
        String[] orderByVals = ((String[]) params.get("order_by"));
        if (orderByVals[0] == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(QueryOrdering.fromOrderBy(orderByVals[0]));
    }

    private Optional<TitleQueryParams> titleQueryFrom(Query<Content> query) throws QueryExecutionException {
        Optional<String> fuzzySearchString = getFuzzySearchString(query);
        if (!fuzzySearchString.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(new TitleQueryParams(fuzzySearchString.get(), getTitleBoostFrom(query)));
    }

    private Optional<Float> getTitleBoostFrom(Query<Content> query) throws QueryExecutionException {
        Map params = query.getContext().getRequest().getParameterMap();
        Object param = params.get("title_boost");
        if (param == null) {
            return Optional.empty();
        }
        String[] titleBoost = (String[]) param;
        if (titleBoost.length > 1) {
            throw new QueryExecutionException("Title boost param (titleBoost) has been specified more than once");
        }
        if (titleBoost[0] == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(Float.parseFloat(titleBoost[0]));
    }

    private Optional<String> getFuzzySearchString(Query<Content> query) throws QueryExecutionException {
        Map params = query.getContext().getRequest().getParameterMap();
        if (!params.containsKey("q")) {
            return Optional.empty();
        }
        String[] searchParam = ((String[]) params.get("q"));
        if (searchParam.length > 1) {
            throw new QueryExecutionException("Search param (q) has been specified more than once");
        }
        if (searchParam[0] == null) {
            return Optional.empty();
        }
        return Optional.of(searchParam[0]);
    }

    private AsyncFunction<FluentIterable<Id>, ResolvedEquivalents<Content>> toEquivalentContent(
            final Query<Content> query) {
        return input -> resolver.resolveIds(input, applicationSources(query), annotations(query));
    }

    private Function<ResolvedEquivalents<Content>, QueryResult<Content>> toQueryResult(final Query<Content> query) {
        return input -> {
            Iterable<Content> resources = input.getFirstElems();
            return QueryResult.listResult(resources, query.getContext());
        };
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
