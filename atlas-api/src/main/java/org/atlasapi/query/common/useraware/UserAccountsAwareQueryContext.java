package org.atlasapi.query.common.useraware;

import java.util.Objects;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import com.google.common.base.MoreObjects;
import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.query.annotation.ActiveAnnotations;

import com.metabroadcast.common.query.Selection;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class UserAccountsAwareQueryContext {

    public static final UserAccountsAwareQueryContext standard(HttpServletRequest request) {
        return new UserAccountsAwareQueryContext(
                null, //TODO: need to be default application
                ActiveAnnotations.standard(),
                ImmutableSet.of(),
                request
        );
    }

    private final Application application;
    private final ActiveAnnotations annotations;
    private final Set<User> userAccounts;
    private final Optional<Selection> selection;
    private final HttpServletRequest request;

    public UserAccountsAwareQueryContext(Application application, ActiveAnnotations annotations,
            Set<User> userAccounts, HttpServletRequest request) {
        this(application, annotations, userAccounts, null, request);
    }

    public UserAccountsAwareQueryContext(Application application, ActiveAnnotations annotations,
            Set<User> userAccounts, Selection selection, HttpServletRequest request) {
        this.application = checkNotNull(application);
        this.annotations = checkNotNull(annotations);
        this.userAccounts = checkNotNull(userAccounts);
        this.selection = Optional.fromNullable(selection);
        this.request = checkNotNull(request);
    }

    public Application getApplication() {
        return this.application;
    }

    public ActiveAnnotations getAnnotations() {
        return this.annotations;
    }

    public Set<User> getUserAccounts() {
        return userAccounts;
    }

    public Optional<Selection> getSelection() {
        return this.selection;
    }

    public HttpServletRequest getRequest() {
        return request;
    }

    public boolean isAdminUser() {
        return this.getUserAccounts()
                .stream()
                .anyMatch(user -> Objects.equals(user.getRole(), Role.ADMIN));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserAccountsAwareQueryContext that = (UserAccountsAwareQueryContext) o;
        return java.util.Objects.equals(application, that.application) &&
                java.util.Objects.equals(annotations, that.annotations) &&
                java.util.Objects.equals(userAccounts, that.userAccounts) &&
                java.util.Objects.equals(selection, that.selection) &&
                java.util.Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(application, annotations, userAccounts, selection, request);
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("application", application)
                .add("annotations", annotations)
                .add("userAccounts", userAccounts)
                .add("selection", selection)
                .toString();
    }
}
