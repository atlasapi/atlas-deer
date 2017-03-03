package org.atlasapi.query.common.context;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.application.ApplicationResolutionException;
import org.atlasapi.application.DefaultApplication;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.query.annotation.AnnotationsExtractor;
import org.atlasapi.query.common.ParameterNameProvider;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryContextParser implements ParameterNameProvider {

    private final ApplicationFetcher configFetcher;
    private final AnnotationsExtractor annotationExtractor;
    private final SelectionBuilder selectionBuilder;

    public QueryContextParser(
            ApplicationFetcher configFetcher,
            AnnotationsExtractor annotationsParser,
            Selection.SelectionBuilder selectionBuilder
    ) {
        this.configFetcher = checkNotNull(configFetcher);
        this.annotationExtractor = checkNotNull(annotationsParser);
        this.selectionBuilder = checkNotNull(selectionBuilder);
    }

    public static QueryContextParser create(
            ApplicationFetcher configFetcher,
            AnnotationsExtractor annotationsParser,
            SelectionBuilder selectionBuilder
    ) {
        return new QueryContextParser(
                configFetcher,
                annotationsParser,
                selectionBuilder
        );
    }

    public QueryContext parseSingleContext(HttpServletRequest request)
            throws QueryParseException, ApplicationResolutionException {
        return QueryContext.create(
                configFetcher.applicationFor(request).orElse(DefaultApplication.create()),
                annotationExtractor.extractFromSingleRequest(request),
                selectionBuilder.build(request),
                request
        );
    }

    public QueryContext parseListContext(HttpServletRequest request)
            throws QueryParseException, ApplicationResolutionException {
        return QueryContext.create(
                configFetcher.applicationFor(request).orElse(DefaultApplication.create()),
                annotationExtractor.extractFromListRequest(request),
                selectionBuilder.build(request),
                request
        );
    }

    @Override
    public ImmutableSet<String> getOptionalParameters() {
        // These have to be maintained for reasons of backwards compatibility. Some of
        // them are no longer being used and some have been properly added to the
        // AttributeQueryParsers for some endpoints where it is appropriate,
        // but not for others where they are not wanted.
        //
        // Removing them from this list will cause calls that improperly reference them
        // to fail whereas before they were just ignored. Therefore we need to preserve
        // them here to ensure those calls continue to work.
        ImmutableSet<String> backwardsCompatibilityList = ImmutableSet.of(
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
                "actionableFilterParameters",
                "platform",
                "downweigh"
        );
        return ImmutableSet.copyOf(
                Iterables.concat(
                        ImmutableList.of(
                                annotationExtractor.getParameterNames(),
                                selectionBuilder.getParameterNames(),
                                configFetcher.getParameterNames(),
                                backwardsCompatibilityList
                        )
                )
        );
    }

    @Override
    public Set<String> getRequiredParameters() {
        return ImmutableSet.of();
    }
}
