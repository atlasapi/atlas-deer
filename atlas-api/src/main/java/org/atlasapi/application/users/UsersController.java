package org.atlasapi.application.users;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.entity.Id;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.InsufficientPrivilegeException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParserNoAuth;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class UsersController {

    private static Logger log = LoggerFactory.getLogger(UsersController.class);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final UserAwareQueryParser<User> requestParser;
    private final StandardUserAwareQueryParserNoAuth<User> requestParserNoAuth;
    private final UserAwareQueryExecutor<User> queryExecutor;
    private final UserAwareQueryResultWriter<User> resultWriter;
    private final UserAccountsAwareQueryExecutor<User> queryExecutorNoAuth;
    private final UserAccountsAwareQueryResultWriter<User> resultWriterNoAuth;
    private final ModelReader reader;
    private final NumberToShortStringCodec idCodec;
    private final UserFetcher userFetcher;
    private final NoAuthUserFetcher userFetcherNoAuth;
    private final UserStore userStore;
    private final Clock clock;

    public UsersController(UserAwareQueryParser<User> requestParser,
            StandardUserAwareQueryParserNoAuth<User> requestParserNoAuth,
            UserAwareQueryExecutor<User> queryExecutor,
            UserAwareQueryResultWriter<User> resultWriter,
            UserAccountsAwareQueryExecutor<User> queryExecutorNoAuth,
            UserAccountsAwareQueryResultWriter<User> resultWriterNoAuth,
            ModelReader reader,
            NumberToShortStringCodec idCodec,
            UserFetcher userFetcher,
            NoAuthUserFetcher userFetcherNoAuth,
            UserStore userStore,
            Clock clock) {
        this.requestParserNoAuth = requestParserNoAuth;
        this.requestParser = requestParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.queryExecutorNoAuth = queryExecutorNoAuth;
        this.resultWriterNoAuth = resultWriterNoAuth;
        this.reader = reader;
        this.idCodec = idCodec;
        this.userFetcher = userFetcher;
        this.userFetcherNoAuth = userFetcherNoAuth;
        this.userStore = userStore;
        this.clock = clock;
    }

    @RequestMapping({ "/4/users/{uid}.*", "/4/users.*" })
    public void outputUsers(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAwareQuery<User> applicationsQuery = requestParser.parse(request);
            UserAwareQueryResult<User> queryResult = queryExecutor.execute(applicationsQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping({ "/4/admin/users/{uid}.*", "/4/admin/users.*" })
    public void outputUsersNoAuth(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAccountsAwareQuery<User> applicationsQuery = requestParserNoAuth.parse(request);
            UserAccountsAwareQueryResult<User> queryResult = queryExecutorNoAuth.execute(applicationsQuery);
            resultWriterNoAuth.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    // If user posts to this endpoint with the oauth token then they are accepting the 
    // terms and conditions
    @RequestMapping(value = "/4/users/{uid}/eula/accept.*", method = RequestMethod.POST)
    public void userAcceptsLicense(HttpServletRequest request, HttpServletResponse response,
            @PathVariable String uid) throws IOException {

        Id userId = Id.valueOf(idCodec.decode(uid));
        boolean hasPermissionToEditUser = hasPermissionToEditUser(
                userId, userFetcher.userFor(request).get()
        );
        userAcceptsLicenseInternal(request, response, userId, hasPermissionToEditUser);
    }

    @RequestMapping(value = "/4/admin/users/{uid}/eula/accept.*", method = RequestMethod.POST)
    public void userAcceptsLicenseNoAuth(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String uid) throws IOException {

        Id userId = Id.valueOf(idCodec.decode(uid));
        boolean hasPermissionToEditUser = hasPermissionToEditUser(
                userId, userFetcherNoAuth.userFor(request)
        );
        userAcceptsLicenseInternal(request, response, userId, hasPermissionToEditUser);
    }

    private void userAcceptsLicenseInternal(HttpServletRequest request,
            HttpServletResponse response, Id userId, boolean hasPermissionToEditUser)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            if (!hasPermissionToEditUser) {
                throw new ResourceForbiddenException();
            }
            Optional<User> existing = userStore.userForId(userId);
            if (!existing.isPresent()) {
                throw new NotFoundException(userId);
            }
            User modified = existing.get().copy().withLicenseAccepted(clock.now()).build();
            UserAwareQueryResult<User> queryResult = UserAwareQueryResult.singleResult(
                    modified,
                    UserAwareQueryContext.standard(request)
            );
            resultWriter.write(queryResult, writer);
            userStore.store(modified);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/users/{uid}.*", method = RequestMethod.POST)
    public void updateUser(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String uid) throws IOException {

        Id userId = Id.valueOf(idCodec.decode(uid));
        User editingUser = userFetcher.userFor(request).get();

        boolean hasPermissionToEditUser = hasPermissionToEditUser(userId, editingUser);
        boolean editingUserIsAdmin = isAdmin(editingUser);

        updateUserInternal(
                request, response, userId, hasPermissionToEditUser, editingUserIsAdmin
        );
    }

    @RequestMapping(value = "/4/admin/users/{uid}.*", method = RequestMethod.POST)
    public void updateUserNoAuth(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String uid) throws IOException {

        Id userId = Id.valueOf(idCodec.decode(uid));
        Set<User> editingUsers = userFetcherNoAuth.userFor(request);

        boolean hasPermissionToEditUser = hasPermissionToEditUser(userId, editingUsers);
        boolean editingUserIsAdmin = isAdmin(editingUsers);

        updateUserInternal(
                request, response, userId, hasPermissionToEditUser, editingUserIsAdmin
        );
    }

    private void updateUserInternal(HttpServletRequest request, HttpServletResponse response,
            Id userId, boolean hasPermissionToEditUser, boolean editingUserIsAdmin)
            throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);

            if (!hasPermissionToEditUser) {
                throw new ResourceForbiddenException();
            }
            Optional<User> existing = userStore.userForId(userId);
            if (existing.isPresent()) {
                User posted = deserialize(
                        new InputStreamReader(request.getInputStream()),
                        User.class
                );

                // Only admins can change the role for a user
                // if editing user is not an admin reject
                if (!editingUserIsAdmin && isUserRoleChanged(posted, existing.get())) {
                    throw new InsufficientPrivilegeException(
                            "You do not have permission to change the user role");
                }

                User modified = updateProfileFields(posted, existing.get());
                userStore.store(modified);
                UserAwareQueryResult<User> queryResult = UserAwareQueryResult.singleResult(
                        modified,
                        UserAwareQueryContext.standard(request)
                );
                resultWriter.write(queryResult, writer);
            } else {
                throw new NotFoundException(userId);
            }
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/users/{uid}.*", method = RequestMethod.DELETE)
    public void deactivateUser(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String uid) throws IOException {

        Id userId = Id.valueOf(idCodec.decode(uid));
        User editingUser = userFetcher.userFor(request).get();

        boolean editingUserIsAdmin = isAdmin(editingUser);

        deactivateUserInternal(request, response, userId, editingUserIsAdmin);
    }

    @RequestMapping(value = "/4/admin/users/{uid}.*", method = RequestMethod.DELETE)
    public void deactivateUserNoAuth(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String uid) throws IOException {

        Id userId = Id.valueOf(idCodec.decode(uid));
        Set<User> editingUsers = userFetcherNoAuth.userFor(request);

        boolean editingUserIsAdmin = isAdmin(editingUsers);

        deactivateUserInternal(request, response, userId, editingUserIsAdmin);
    }

    private void deactivateUserInternal(HttpServletRequest request, HttpServletResponse response,
            Id userId, boolean editingUserIsAdmin) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);

            if (!editingUserIsAdmin) {
                throw new ResourceForbiddenException();
            }

            Optional<User> existing = userStore.userForId(userId);
            if (existing.isPresent()) {
                User deactivated = existing.get().copy().withProfileDeactivated(true).build();
                userStore.store(deactivated);
                UserAwareQueryResult<User> queryResult = UserAwareQueryResult.singleResult(
                        deactivated,
                        UserAwareQueryContext.standard(request)
                );
                resultWriter.write(queryResult, writer);
            } else {
                throw new NotFoundException(userId);
            }
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private boolean hasPermissionToEditUser(Id userId, User editingUser) {
        return isAdmin(editingUser) || editingUser.getId().equals(userId);
    }

    private boolean hasPermissionToEditUser(Id userId, Set<User> editingUsers) {
        return editingUsers.stream()
                .anyMatch(user -> hasPermissionToEditUser(userId, user));
    }

    private boolean isAdmin(User user) {
        return user.is(Role.ADMIN);
    }

    private boolean isAdmin(Set<User> users) {
        return users.stream()
                .anyMatch(this::isAdmin);
    }

    private boolean isUserRoleChanged(User posted, User existing) {
        return !posted.getRole().equals(existing.getRole());
    }

    /**
     * Only allow certain fields to be updated
     */
    private User updateProfileFields(User posted, User existing) {
        return existing.copy()
                .withFullName(posted.getFullName())
                .withCompany(posted.getCompany())
                .withEmail(posted.getEmail())
                .withWebsite(posted.getWebsite())
                .withProfileComplete(posted.isProfileComplete())
                .withRole(posted.getRole())
                .build();

    }

    private <T> T deserialize(Reader input, Class<T> cls) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), cls);
    }
}
