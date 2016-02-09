package org.atlasapi.application;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.source.Sources;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class ApplicationQueryExecutor implements UserAwareQueryExecutor<Application> {

    private final ApplicationStore applicationStore;

    public ApplicationQueryExecutor(ApplicationStore applicationStore) {
        this.applicationStore = applicationStore;
    }

    @Override
    public UserAwareQueryResult<Application> execute(UserAwareQuery<Application> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAwareQueryResult<Application> singleQuery(UserAwareQuery<Application> query)
            throws QueryExecutionException {
        Id id = query.getOnlyId();
        if (!userCanAccessApplication(id, query)) {
            throw new ResourceForbiddenException();
        }

        Optional<Application> application = applicationStore.applicationFor(id);
        if (application.isPresent() && query.getContext().isAdminUser()) {
            return UserAwareQueryResult.singleResult(application.get(), query.getContext());
        } else if (application.isPresent() && !query.getContext().isAdminUser()) {
            return UserAwareQueryResult.singleResult(copyApplicationWithAdminOnlySourcesRemoved(
                    application.get()), query.getContext());
        } else {
            throw new NotFoundException(id);
        }

    }

    private UserAwareQueryResult<Application> multipleQuery(UserAwareQuery<Application> query)
            throws QueryExecutionException {
        AttributeQuerySet operands = query.getOperands();
        User user = query.getContext().getUser().get();

        Iterable<Id> ids = Iterables.concat(operands.accept(new QueryVisitorAdapter<List<Id>>() {

            @Override
            public List<Id> visit(IdAttributeQuery query) {
                return query.getValue();
            }
        }));
        Publisher reads = null;

        Publisher writes = null;

        for (AttributeQuery<?> operand : operands) {
            if (operand.getAttributeName().equals(Attributes.SOURCE_READS.externalName())) {
                reads = (Publisher) Iterables.getOnlyElement(operand.getValue());
            } else if (operand.getAttributeName().equals(Attributes.SOURCE_WRITES.externalName())) {
                writes = (Publisher) Iterables.getOnlyElement(operand.getValue());
            }
        }

        Iterable<Application> results = null;

        if (!Iterables.isEmpty(ids)) {
            results = resolve(ids);
        } else if (reads != null) {
            results = applicationStore.allApplications();
        } else if (writes != null) {
            results = applicationStore.writersFor(writes);
        } else {
            if (query.getContext().isAdminUser()) {
                results = applicationStore.allApplications();
            } else {
                results = resolve(user.getApplicationIds());
            }
        }

        if (query.getContext().isAdminUser()) {
            return UserAwareQueryResult.listResult(results, query.getContext());
        } else {
            Iterable<Application> userApplications = filterByUserViewable(results, user);
            return UserAwareQueryResult.listResult(copyApplicationsWithAdminOnlySourcesRemoved(
                    userApplications), query.getContext());
        }
    }

    private FluentIterable<Application> resolve(Iterable<Id> ids) throws QueryExecutionException {
        ListenableFuture<Resolved<Application>> futureApps = applicationStore.resolveIds(ids);
        Resolved<Application> resolved = Futures.get(futureApps, QueryExecutionException.class);
        return resolved.getResources();
    }

    private boolean userCanAccessApplication(Id id, UserAwareQuery<Application> query) {
        Optional<User> user = query.getContext().getUser();
        return user.isPresent() && (user.get().is(Role.ADMIN) || user.get()
                .getApplicationIds()
                .contains(id));
    }

    private Iterable<Application> filterByUserViewable(Iterable<Application> applications,
            final User user) {
        return Iterables.filter(applications, new Predicate<Application>() {

            @Override
            public boolean apply(@Nullable Application input) {
                return user.getApplicationIds().contains(input.getId());
            }
        });
    }

    private Iterable<Application> copyApplicationsWithAdminOnlySourcesRemoved(
            Iterable<Application> applications) {
        return Iterables.transform(applications, new Function<Application, Application>() {

            @Override
            public Application apply(Application input) {
                return copyApplicationWithAdminOnlySourcesRemoved(input);
            }
        });
    }

    private Application copyApplicationWithAdminOnlySourcesRemoved(Application application) {
        List<SourceReadEntry> reads = filterSourcesReadsForNonAdmins(
                application.getSources().getReads());
        return application.copy().withSources(application.getSources()
                .copy().withReadableSources(reads).build())
                .build();
    }

    private List<SourceReadEntry> filterSourcesReadsForNonAdmins(List<SourceReadEntry> reads) {
        return ImmutableList.copyOf(Iterables.filter(reads, new Predicate<SourceReadEntry>() {

            @Override
            public boolean apply(SourceReadEntry input) {
                return !Sources.isAdminOnlySource(input.getPublisher());
            }
        }));
    }
}
