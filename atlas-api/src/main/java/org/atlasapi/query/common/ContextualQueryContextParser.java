package org.atlasapi.query.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

public class ContextualQueryContextParser implements ParameterNameProvider {
    
    private final ApplicationSourcesFetcher configFetcher;
    private final UserFetcher userFetcher;
    private final ContextualAnnotationsExtractor annotationExtractor;
    private final SelectionBuilder selectionBuilder;

    public ContextualQueryContextParser(ApplicationSourcesFetcher configFetcher, UserFetcher userFetcher, ContextualAnnotationsExtractor annotationsParser, Selection.SelectionBuilder selectionBuilder) {
        this.configFetcher = checkNotNull(configFetcher);
        this.userFetcher = userFetcher;
        this.annotationExtractor = checkNotNull(annotationsParser);
        this.selectionBuilder = checkNotNull(selectionBuilder);
    }

    public QueryContext parseContext(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        return new QueryContext(
            configFetcher.sourcesFor(request).or(ApplicationSources.defaults()),
            annotationExtractor.extractFromRequest(request),
            selectionBuilder.build(request),
            request
        );
    }

    @Override
    public ImmutableSet<String> getOptionalParameters() {
        return ImmutableSet.copyOf(Iterables.concat(ImmutableList.of( 
            userFetcher.getParameterNames(),
            annotationExtractor.getParameterNames(), 
            selectionBuilder.getParameterNames(),
            ImmutableSet.of(JsonResponseWriter.CALLBACK))));
    }
    
    @Override
    public Set<String> getRequiredParameters() {
        return configFetcher.getParameterNames();
    }
}
