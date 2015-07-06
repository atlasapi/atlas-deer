package org.atlasapi.query.common.useraware;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.query.annotation.AnnotationsExtractor;
import org.atlasapi.query.common.ParameterNameProvider;
import org.atlasapi.content.QueryParseException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

//WHY DOES THIS EVEN NEED TO FETCH AN APPLICATION?
public class UserAwareQueryContextParser implements ParameterNameProvider {
    
    private final ApplicationSourcesFetcher configFetcher;
    private final UserFetcher userFetcher;
    private final AnnotationsExtractor annotationExtractor;
    private final SelectionBuilder selectionBuilder;

    public UserAwareQueryContextParser(ApplicationSourcesFetcher configFetcher, UserFetcher userFetcher, AnnotationsExtractor annotationsParser, Selection.SelectionBuilder selectionBuilder) {
        this.configFetcher = checkNotNull(configFetcher);
        this.userFetcher = checkNotNull(userFetcher);
        this.annotationExtractor = checkNotNull(annotationsParser);
        this.selectionBuilder = checkNotNull(selectionBuilder);
    }
    
    public UserAwareQueryContext parseSingleContext(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        return new UserAwareQueryContext(
                configFetcher.sourcesFor(request).or(ApplicationSources.defaults()),
                annotationExtractor.extractFromSingleRequest(request),
                userFetcher.userFor(request),
                selectionBuilder.build(request),
                request
                );
    }

    public UserAwareQueryContext parseListContext(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException {
        return new UserAwareQueryContext(
                configFetcher.sourcesFor(request).or(ApplicationSources.defaults()),
            annotationExtractor.extractFromListRequest(request),
            userFetcher.userFor(request),
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
        return ImmutableSet.of();
    }
    
    
}
