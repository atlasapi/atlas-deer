package org.atlasapi.query.common;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.query.annotation.ActiveAnnotations;

import com.metabroadcast.common.query.Selection;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryContext {

    public static final QueryContext standard(HttpServletRequest request) {
        return new QueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                request
        );
    }

    private final ApplicationSources appSources;
    private final ActiveAnnotations annotations;
    private final Optional<Selection> selection;
    private final HttpServletRequest request;

    public QueryContext(
            ApplicationSources appSources,
            ActiveAnnotations annotations,
            HttpServletRequest request
    ) {
        this(appSources, annotations, null, request);
    }

    public QueryContext(
            ApplicationSources appSources,
            ActiveAnnotations annotations,
            Selection selection,
            HttpServletRequest request
    ) {
        this.appSources = checkNotNull(appSources);
        this.annotations = checkNotNull(annotations);
        this.selection = Optional.fromNullable(selection);
        this.request = checkNotNull(request);
    }

    public ApplicationSources getApplicationSources() {
        return this.appSources;
    }

    public ActiveAnnotations getAnnotations() {
        return this.annotations;
    }

    public Optional<Selection> getSelection() {
        return this.selection;
    }

    public HttpServletRequest getRequest() {
        return request;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof QueryContext) {
            QueryContext other = (QueryContext) that;
            return appSources.equals(other.appSources)
                    && annotations.equals(other.annotations)
                    && selection.equals(other.selection);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return appSources.hashCode() ^ annotations.hashCode() ^ selection.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("config", appSources)
                .add("annotations", annotations)
                .add("selection", selection)
                .toString();
    }
}
