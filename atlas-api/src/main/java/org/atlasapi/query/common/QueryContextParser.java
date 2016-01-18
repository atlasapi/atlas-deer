package org.atlasapi.query.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.query.annotation.AnnotationsExtractor;

import javax.servlet.http.HttpServletRequest;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryContextParser implements ParameterNameProvider {

    private final ApplicationSourcesFetcher configFetcher;
    private final UserFetcher userFetcher;
    private final AnnotationsExtractor annotationExtractor;
    private final SelectionBuilder selectionBuilder;

    public QueryContextParser(ApplicationSourcesFetcher configFetcher, UserFetcher userFetcher, AnnotationsExtractor annotationsParser, Selection.SelectionBuilder selectionBuilder) {
        this.configFetcher = checkNotNull(configFetcher);
        this.userFetcher = checkNotNull(userFetcher);
        this.annotationExtractor = checkNotNull(annotationsParser);
        this.selectionBuilder = checkNotNull(selectionBuilder);
    }

    public QueryContext parseSingleContext(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        return new QueryContext(
                configFetcher.sourcesFor(request).or(ApplicationSources.defaults()),
                annotationExtractor.extractFromSingleRequest(request),
                selectionBuilder.build(request),
                request
        );
    }

    public QueryContext parseListContext(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        return new QueryContext(
                configFetcher.sourcesFor(request).or(ApplicationSources.defaults()),
                annotationExtractor.extractFromListRequest(request),
                selectionBuilder.build(request),
                request
        );
    }

    @Override
    public ImmutableSet<String> getOptionalParameters() {
        return ImmutableSet.copyOf(
                Iterables.concat(
                        ImmutableList.of(
                                userFetcher.getParameterNames(),
                                annotationExtractor.getParameterNames(),
                                selectionBuilder.getParameterNames(),
                                configFetcher.getParameterNames(),
                                ImmutableSet.of(
                                        JsonResponseWriter.CALLBACK,
                                        "available",
                                        "order_by",
                                        "q",
                                        "title_boost",
                                        "locations.available",
                                        "broadcasts.transmissionTime.gt",
                                        "broadcasts.transmissionTime.lt",
                                        "broadcasts.transmissionTime.eq",
                                        "broadcasts.transmissionEndTime.gt",
                                        "broadcasts.transmissionEndTime.lt",
                                        "broadcasts.transmissionEndTime.eq",
                                        "region",
                                        "broadcastWeight",
                                        "titleWeight",
                                        "tags.topic.id",
                                        "brand.series.available",
                                        "sub_items.limit",
                                        "sub_items.offset",
                                        "sub_items.ordering",
                                        "sub_items_summaries.limit",
                                        "sub_items_summaries.offset",
                                        "sub_items_summaries.ordering",
                                        "episode.brand.id",
                                        "brand.id",
                                        "series.id",
                                        "actionableFilterParameters"
                                )
                        )
                )
        );
    }

    @Override
    public Set<String> getRequiredParameters() {
        return ImmutableSet.of();
    }

}
