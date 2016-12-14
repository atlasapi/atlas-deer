package org.atlasapi.query.common.useraware;

import javax.servlet.http.HttpServletRequest;

import com.google.common.base.MoreObjects;
import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.query.annotation.ActiveAnnotations;

import com.metabroadcast.common.query.Selection;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class UserAwareQueryContext {

    public static final UserAwareQueryContext standard(HttpServletRequest request) {
        return new UserAwareQueryContext(
                null, //TODO: needs to be default application
                ActiveAnnotations.standard(),
                Optional.<User>absent(),
                request
        );
    }

    private final Application application;
    private final ActiveAnnotations annotations;
    private final Optional<User> user;
    private final Optional<Selection> selection;
    private final HttpServletRequest request;

    public UserAwareQueryContext(Application application, ActiveAnnotations annotations,
            Optional<User> user, HttpServletRequest request) {
        this(application, annotations, user, null, request);
    }

    public UserAwareQueryContext(Application application, ActiveAnnotations annotations,
            Optional<User> user, Selection selection, HttpServletRequest request) {
        this.application = checkNotNull(application);
        this.annotations = checkNotNull(annotations);
        this.user = checkNotNull(user);
        this.selection = Optional.fromNullable(selection);
        this.request = checkNotNull(request);
    }

    public Application getApplication() {
        return this.application;
    }

    public ActiveAnnotations getAnnotations() {
        return this.annotations;
    }

    public Optional<User> getUser() {
        return user;
    }

    public Optional<Selection> getSelection() {
        return this.selection;
    }

    public HttpServletRequest getRequest() {
        return request;
    }

    public boolean isAdminUser() {
        return this.getUser().get().getRole().equals(Role.ADMIN);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof UserAwareQueryContext) {
            UserAwareQueryContext other = (UserAwareQueryContext) that;
            return application.equals(other.application)
                    && annotations.equals(other.annotations)
                    && user.equals(other.user)
                    && selection.equals(other.selection);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return application.hashCode() ^ annotations.hashCode() ^ user.hashCode()
                ^ selection.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("application", application)
                .add("annotations", annotations)
                .add("user", user)
                .add("selection", selection)
                .toString();
    }
}
