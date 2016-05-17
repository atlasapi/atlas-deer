package org.atlasapi.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.entity.Id;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAuthorizedException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParserNoAuth;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class ApplicationsController {

    private static Logger log = LoggerFactory.getLogger(ApplicationsController.class);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final UserAwareQueryParser<Application> requestParser;
    private final StandardUserAwareQueryParserNoAuth<Application> requestParserNoAuth;
    private final UserAccountsAwareQueryExecutor<Application> queryExecutorNoAuth;
    private final UserAwareQueryExecutor<Application> queryExecutor;
    private final UserAwareQueryResultWriter<Application> resultWriter;
    private final UserAccountsAwareQueryResultWriter<Application> resultWriterNoAuth;
    private final ModelReader reader;
    private final NumberToShortStringCodec idCodec;
    private final SourceIdCodec sourceIdCodec;
    private final ApplicationStore applicationStore;
    private final UserFetcher userFetcher;
    private final NoAuthUserFetcher userFetcherNoAuth;
    private final UserStore userStore;

    private static class PrecedenceOrdering {

        private List<String> ordering;

        public List<String> getOrdering() {
            return ordering;
        }
    }

    public ApplicationsController(UserAwareQueryParser<Application> requestParser,
            StandardUserAwareQueryParserNoAuth<Application> requestParserNoAuth,
            UserAwareQueryExecutor<Application> queryExecutor,
            UserAccountsAwareQueryExecutor<Application> queryExecutorNoAuth,
            UserAwareQueryResultWriter<Application> resultWriter,
            UserAccountsAwareQueryResultWriter<Application> resultWriterNoAuth,
            ModelReader reader,
            NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec,
            ApplicationStore applicationStore,
            UserFetcher userFetcher,
            NoAuthUserFetcher userFetcherNoAuth,
            UserStore userStore) {
        this.requestParser = requestParser;
        this.requestParserNoAuth = requestParserNoAuth;
        this.queryExecutor = queryExecutor;
        this.queryExecutorNoAuth = queryExecutorNoAuth;
        this.resultWriter = resultWriter;
        this.resultWriterNoAuth = resultWriterNoAuth;
        this.reader = reader;
        this.idCodec = idCodec;
        this.sourceIdCodec = sourceIdCodec;
        this.applicationStore = applicationStore;
        this.userFetcher = userFetcher;
        this.userFetcherNoAuth = userFetcherNoAuth;
        this.userStore = userStore;
    }

    @RequestMapping({ "/4/applications/{aid}.*", "/4/applications.*" })
    public void outputAllApplications(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAwareQuery<Application> applicationsQuery = requestParser.parse(request);
            UserAwareQueryResult<Application> queryResult = queryExecutor.execute(applicationsQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }


    @RequestMapping({ "/4/admin/applications/{aid}.*", "/4/admin/applications.*" })
    public void outputAllApplicationsNoAuth(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAccountsAwareQuery<Application> applicationsQuery = requestParserNoAuth.parse(request);
            UserAccountsAwareQueryResult<Application> queryResult = queryExecutorNoAuth.execute(applicationsQuery);
            resultWriterNoAuth.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }


    @RequestMapping(value = "/4/applications.*", method = RequestMethod.POST)
    public void writeApplication(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Application application = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    Application.class
            );
            User user = userFetcher.userFor(request).get();
            if (application.getId() != null) {
                if (!userCanAccessApplication(application.getId(), request, userFetcher)) {
                    throw new ResourceForbiddenException();
                }
                Optional<Application> existing = applicationStore.applicationFor(application.getId());
                checkSourceStatusChanges(user, application, existing);
                // Copy across slug and disallow modification of credentials
                application = application.copy().withSlug(existing.get().getSlug())
                        .withCredentials(existing.get().getCredentials()).build();
                application = applicationStore.updateApplication(application);
            } else {
                checkSourceStatusChanges(user, application, Optional.<Application>absent());
                // New application
                application = applicationStore.createApplication(application);
                // Add application to user ownership
                user = user.copyWithAdditionalApplication(application);
                userStore.store(user);
            }
            // We do not want non-admins to see admin only sources
            // So we run the application through the query executor
            // to filter out anything they should not see
            UserAwareQueryContext context = new UserAwareQueryContext(
                    ApplicationSources.defaults(),
                    ActiveAnnotations.standard(),
                    Optional.of(user),
                    request
            );
            UserAwareQuery<Application> applicationsQuery = UserAwareQuery.singleQuery(
                    application.getId(),
                    context
            );
            UserAwareQueryResult<Application> queryResult = queryExecutor.execute(applicationsQuery);

            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/admin/applications.*", method = RequestMethod.POST)
    public void writeApplicationNoAuth(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Application application = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    Application.class
            );
            Set<User> userAccounts = userFetcherNoAuth.userFor(request);
            Set<User> userAccountsWithNewApplication = Sets.newHashSet();
            if (application.getId() != null) {
                if (!userCanAccessApplication(application.getId(), request, userFetcherNoAuth)) {
                    throw new ResourceForbiddenException();
                }
                Optional<Application> existing = applicationStore.applicationFor(application.getId());
                checkSourceStatusChanges(userAccounts, application, existing);
                // Copy across slug and disallow modification of credentials
                application = application.copy().withSlug(existing.get().getSlug())
                        .withCredentials(existing.get().getCredentials()).build();
                application = applicationStore.updateApplication(application);
            } else {
                checkSourceStatusChanges(userAccounts, application, Optional.<Application>absent());
                // New application
                application = applicationStore.createApplication(application);
                // Add application to user ownership
                for (User userAccount : userAccounts) {
                    userAccount = userAccount.copyWithAdditionalApplication(application);
                    userStore.store(userAccount);
                    userAccountsWithNewApplication.add(userAccount);
                }

            }
            // We do not want non-admins to see admin only sources
            // So we run the application through the query executor
            // to filter out anything they should not see
            UserAccountsAwareQueryContext context = new UserAccountsAwareQueryContext(
                    ApplicationSources.defaults(),
                    ActiveAnnotations.standard(),
                    userAccountsWithNewApplication,
                    request
            );
            UserAccountsAwareQuery<Application> applicationsQuery = UserAccountsAwareQuery.singleQuery(
                    application.getId(),
                    context
            );
            UserAccountsAwareQueryResult<Application> queryResult = queryExecutorNoAuth.execute(applicationsQuery);

            resultWriterNoAuth.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/applications/{aid}/sources", method = RequestMethod.POST)
    public void writeApplicationSources(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String aid)
            throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        ApplicationSources sources;
        User user = userFetcher.userFor(request).get();
        try {
            if (!userCanAccessApplication(applicationId, request, userFetcher)) {
                throw new ResourceForbiddenException();
            }
            sources = deserialize(new InputStreamReader(
                    request.getInputStream()), ApplicationSources.class);
            Optional<Application> existing = applicationStore.applicationFor(applicationId);
            Application modified = existing.get().copyWithSources(sources);
            checkSourceStatusChanges(user, modified, existing);
            applicationStore.updateApplication(modified);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

    @RequestMapping(value = "/4/admin/applications/{aid}/sources", method = RequestMethod.POST)
    public void writeApplicationSourcesNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String aid)
            throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        ApplicationSources sources;
        Set<User> userAccounts = userFetcherNoAuth.userFor(request);
        try {
            if (!userCanAccessApplication(applicationId, request, userFetcherNoAuth)) {
                throw new ResourceForbiddenException();
            }
            sources = deserialize(new InputStreamReader(
                    request.getInputStream()), ApplicationSources.class);
            Optional<Application> existing = applicationStore.applicationFor(applicationId);
            Application modified = existing.get().copyWithSources(sources);
            checkSourceStatusChanges(userAccounts, modified, existing);
            applicationStore.updateApplication(modified);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }


    @RequestMapping(value = "/4/applications/{aid}/precedence", method = RequestMethod.POST)
    public void setPrecedenceOrder(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String aid) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        PrecedenceOrdering ordering;
        try {
            if (!userCanAccessApplication(applicationId, request, userFetcher)) {
                throw new ResourceForbiddenException();
            }
            ordering = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    PrecedenceOrdering.class
            );
            List<Publisher> sourceOrder;
            sourceOrder = getSourcesFrom(ordering);
            Application existing = applicationStore.applicationFor(applicationId).get();
            applicationStore.updateApplication(existing.copyWithReadSourceOrder(sourceOrder));
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

    @RequestMapping(value = "/4/admin/applications/{aid}/precedence", method = RequestMethod.POST)
    public void setPrecedenceOrderNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String aid) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        Id applicationId = Id.valueOf(idCodec.decode(aid));
        PrecedenceOrdering ordering;
        try {
            if (!userCanAccessApplication(applicationId, request, userFetcherNoAuth)) {
                throw new ResourceForbiddenException();
            }
            ordering = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    PrecedenceOrdering.class
            );
            List<Publisher> sourceOrder;
            sourceOrder = getSourcesFrom(ordering);
            Application existing = applicationStore.applicationFor(applicationId).get();
            applicationStore.updateApplication(existing.copyWithReadSourceOrder(sourceOrder));
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

    @RequestMapping(value = "/4/applications/{aid}/precedence", method = RequestMethod.DELETE)
    public void disablePrecedence(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String aid) throws IOException {
        try {
            response.addHeader("Access-Control-Allow-Origin", "*");
            Id applicationId = Id.valueOf(idCodec.decode(aid));
            if (!userCanAccessApplication(applicationId, request, userFetcher)) {
                throw new ResourceForbiddenException();
            }
            Application existing = applicationStore.applicationFor(applicationId).get();
            applicationStore.updateApplication(existing.copyWithPrecedenceDisabled());
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

    @RequestMapping(value = "/4/admin/applications/{aid}/precedence", method = RequestMethod.DELETE)
    public void disablePrecedenceNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String aid) throws IOException {
        try {
            response.addHeader("Access-Control-Allow-Origin", "*");
            Id applicationId = Id.valueOf(idCodec.decode(aid));
            if (!userCanAccessApplication(applicationId, request, userFetcherNoAuth)) {
                throw new ResourceForbiddenException();
            }
            Application existing = applicationStore.applicationFor(applicationId).get();
            applicationStore.updateApplication(existing.copyWithPrecedenceDisabled());
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

    private <T> T deserialize(Reader input, Class<T> cls) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), cls);
    }

    private List<Publisher> getSourcesFrom(PrecedenceOrdering ordering)
            throws QueryExecutionException {
        ImmutableList.Builder<Publisher> sources = ImmutableList.builder();
        for (String sourceId : ordering.getOrdering()) {
            Optional<Publisher> source = sourceIdCodec.decode(sourceId);
            if (source.isPresent()) {
                sources.add(source.get());
            } else {
                throw new QueryExecutionException("No publisher by id " + sourceId);
            }
        }
        return sources.build();
    }

    private boolean userCanAccessApplication(Id id, HttpServletRequest request,
            UserFetcher userFetcher) {
        Optional<User> user = userFetcher.userFor(request);
        if (!user.isPresent()) {
            return false;
        } else {
            return user.get().is(Role.ADMIN) || user.get().getApplicationIds().contains(id);
        }
    }

    private boolean userCanAccessApplication(Id id, HttpServletRequest request,
            NoAuthUserFetcher userFetcher) {
        Set<User> userAccounts = userFetcher.userFor(request);
        return userAccounts.stream().filter(user -> user.is(Role.ADMIN) || user.getApplicationIds().contains(id)).findAny().isPresent();
    }

    // Restrict source status changes that non admins can make
    private void checkSourceStatusChanges(User user, Application application,
            Optional<Application> existingApp) throws NotAuthorizedException {
        // An admin can always make a source status change
        // Additionally the sources object may be null here if creating an application
        // and the element has not been specified
        if (user.is(Role.ADMIN) || application.getSources() == null) {
            return;
        }
        // cycle through source reads and check status changes are allowed
        checkAllSources(application, existingApp);
    }

    private void checkSourceStatusChanges(Set<User> userAccounts, Application application,
            Optional<Application> existingApp) throws NotAuthorizedException {
        // An admin can always make a source status change
        // Additionally the sources object may be null here if creating an application
        // and the element has not been specified
        if (userAccounts.stream()
                .filter(user -> user.is(Role.ADMIN))
                .findAny()
                .isPresent() || application.getSources() == null) {
            return;
        }
        // cycle through source reads and check status changes are allowed
        checkAllSources(application, existingApp);
    }



    private void checkAllSources(Application application, Optional<Application> existingApp)
            throws NotAuthorizedException {
        for (SourceReadEntry read : application.getSources().getReads()) {
            SourceStatus existing;
            if (existingApp.isPresent()) {
                existing = existingApp.get().getSources().readStatusOrDefault(read.getPublisher());
            } else {
                existing = SourceStatus.fromV3SourceStatus(read.getPublisher()
                        .getDefaultSourceStatus());
            }
            // Only allow non admins to enable or disable a source
            if (isStateChanged(existing, read.getSourceStatus())) {
                throw new NotAuthorizedException();
            }
        }
    }

    private boolean isStateChanged(SourceStatus existing, SourceStatus submitted) {
        return !submitted.getState().equals(existing.getState());
    }
}
