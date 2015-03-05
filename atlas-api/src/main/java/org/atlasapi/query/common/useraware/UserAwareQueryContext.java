package org.atlasapi.query.common.useraware;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.query.annotation.ActiveAnnotations;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.metabroadcast.common.query.Selection;

import javax.servlet.http.HttpServletRequest;

public class UserAwareQueryContext {

    public static final UserAwareQueryContext standard(HttpServletRequest request) {
        return new UserAwareQueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                Optional.<User>absent(),
                request);
    }
    
    private final ApplicationSources appSources;
    private final ActiveAnnotations annotations;
    private final Optional<User> user;
    private final Optional<Selection> selection;
    private final HttpServletRequest request;

    public UserAwareQueryContext(ApplicationSources appSources, ActiveAnnotations annotations, Optional<User> user, HttpServletRequest request) {
        this(appSources, annotations, user, null, request);
    }
    
    public UserAwareQueryContext(ApplicationSources appSources, ActiveAnnotations annotations,
        Optional<User> user, Selection selection,HttpServletRequest request) {
        this.appSources = checkNotNull(appSources);
        this.annotations = checkNotNull(annotations);
        this.user = checkNotNull(user);
        this.selection = Optional.fromNullable(selection);
        this.request = checkNotNull(request);
    }

    public ApplicationSources getApplicationSources() {
        return this.appSources;
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
            return appSources.equals(other.appSources)
                && annotations.equals(other.annotations)
                && user.equals(other.user)
                && selection.equals(other.selection);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return appSources.hashCode() ^ annotations.hashCode() ^ user.hashCode()
                ^ selection.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("config", appSources)
            .add("annotations", annotations)
            .add("user", user)
            .add("selection", selection)
            .toString();
    }
}
