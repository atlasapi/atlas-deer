package org.atlasapi.query.common;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.ApplicationFetcher;
import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContextualQueryContextParser implements ParameterNameProvider {

    private final ApplicationFetcher configFetcher;
    private final UserFetcher userFetcher;
    private final ContextualAnnotationsExtractor annotationExtractor;
    private final SelectionBuilder selectionBuilder;

    public ContextualQueryContextParser(ApplicationFetcher configFetcher,
            UserFetcher userFetcher, ContextualAnnotationsExtractor annotationsParser,
            Selection.SelectionBuilder selectionBuilder) {
        this.configFetcher = checkNotNull(configFetcher);
        this.userFetcher = userFetcher;
        this.annotationExtractor = checkNotNull(annotationsParser);
        this.selectionBuilder = checkNotNull(selectionBuilder);
    }

    public QueryContext parseContext(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return new QueryContext(
                configFetcher.applicationFor(request).orElse(configFetcher.getDefaults()),
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
                configFetcher.getParameterNames(),
                ImmutableSet.of(JsonResponseWriter.CALLBACK)
        )));
    }

    @Override
    public Set<String> getRequiredParameters() {
        return ImmutableSet.of();
    }
}
