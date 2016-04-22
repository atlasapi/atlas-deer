package org.atlasapi.query.common.useraware;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.query.annotation.AnnotationsExtractor;
import org.atlasapi.query.common.ParameterNameProvider;

import com.metabroadcast.common.query.Selection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class UserAwareQueryContextParserNoAuth implements ParameterNameProvider {

    private final ApplicationSourcesFetcher configFetcher;
    private final NoAuthUserFetcher userFetcher;
    private final AnnotationsExtractor annotationExtractor;
    private final Selection.SelectionBuilder selectionBuilder;

    public UserAwareQueryContextParserNoAuth(ApplicationSourcesFetcher configFetcher,
            NoAuthUserFetcher userFetcher, AnnotationsExtractor annotationsParser,
            Selection.SelectionBuilder selectionBuilder) {
        this.configFetcher = checkNotNull(configFetcher);
        this.userFetcher = checkNotNull(userFetcher);
        this.annotationExtractor = checkNotNull(annotationsParser);
        this.selectionBuilder = checkNotNull(selectionBuilder);
    }

    public UserAccountsAwareQueryContext parseSingleContext(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return new UserAccountsAwareQueryContext(
                configFetcher.sourcesFor(request).or(ApplicationSources.defaults()),
                annotationExtractor.extractFromSingleRequest(request),
                userFetcher.userFor(request),
                selectionBuilder.build(request),
                request
        );
    }

    public UserAccountsAwareQueryContext parseListContext(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return new UserAccountsAwareQueryContext(
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
                ImmutableSet.of(JsonResponseWriter.CALLBACK)
        )));
    }

    @Override
    public Set<String> getRequiredParameters() {
        return ImmutableSet.of();
    }

}
