package org.atlasapi.query.common.context;

import java.util.Set;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.DefaultApplication;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.query.annotation.ActiveAnnotations;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryContext {

    private final Application application;
    private final ActiveAnnotations annotations;
    private final Set<AttributeQuery<?>> operands;
    private final Optional<Selection> selection;
    private final HttpServletRequest request;

    private QueryContext(
            Application application,
            ActiveAnnotations annotations,
            Set<AttributeQuery<?>> operands,
            @Nullable Selection selection,
            HttpServletRequest request
    ) {
        this.application = checkNotNull(application);
        this.annotations = checkNotNull(annotations);
        this.operands = checkNotNull(operands);
        this.selection = Optional.fromNullable(selection);
        this.request = checkNotNull(request);
    }


    public static QueryContext standard(HttpServletRequest request) {
        return new QueryContext(
                DefaultApplication.create(),
                ActiveAnnotations.standard(),
                ImmutableSet.of(),
                null,
                request
        );
    }

    public static QueryContext create(
            Application application,
            ActiveAnnotations annotations,
            HttpServletRequest request
    ) {
        return new QueryContext(
                application,
                annotations,
                ImmutableSet.of(),
                null,
                request
        );
    }

    public static QueryContext create(
            Application application,
            ActiveAnnotations annotations,
            Selection selection,
            HttpServletRequest request
    ) {
        return new QueryContext(
                application,
                annotations,
                ImmutableSet.of(),
                selection,
                request
        );
    }

    public static QueryContext create(
            Application application,
            ActiveAnnotations annotations,
            Set<AttributeQuery<?>> operands,
            HttpServletRequest request
    ) {
        return new QueryContext(application, annotations, operands, null, request);
    }

    public Application getApplication() {
        return this.application;
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

    public Set<AttributeQuery<?>> getOperands() {
        return operands;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof QueryContext) {
            QueryContext other = (QueryContext) that;
            return application.equals(other.application)
                    && annotations.equals(other.annotations)
                    && selection.equals(other.selection);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return application.hashCode() ^ annotations.hashCode() ^ selection.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("application", application)
                .add("annotations", annotations)
                .add("selection", selection)
                .add("operands", operands)
                .toString();
    }
}
