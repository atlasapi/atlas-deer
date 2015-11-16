package org.atlasapi.application.users;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Optional;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.social.auth.credentials.Credentials;
import com.metabroadcast.common.social.auth.credentials.CredentialsStore;
import com.metabroadcast.common.social.auth.credentials.MongoDBCredentialsStore;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.time.Clock;

@Controller
public class UsersController {
    private static Logger log = LoggerFactory.getLogger(UsersController.class);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final UserAwareQueryParser<User> requestParser;
    private final UserAwareQueryExecutor<User> queryExecutor;
    private final UserAwareQueryResultWriter<User> resultWriter;
    private final ModelReader reader;
    private final NumberToShortStringCodec idCodec;
    private final UserFetcher userFetcher;
    private final UserStore userStore;
    private final MongoDBCredentialsStore credentialsStore;
    private final Clock clock;
    
    public UsersController(UserAwareQueryParser<User> requestParser,
            UserAwareQueryExecutor<User> queryExecutor, 
            UserAwareQueryResultWriter<User> resultWriter,
            ModelReader reader,
            NumberToShortStringCodec idCodec,
            UserFetcher userFetcher,
            UserStore userStore,
            CredentialsStore credentialsStore,
            Clock clock) {
        this.requestParser = requestParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.reader = reader;
        this.idCodec = idCodec;
        this.userFetcher = userFetcher;
        this.userStore = userStore;
        this.clock = clock;
        this.credentialsStore = (MongoDBCredentialsStore) credentialsStore;
    }

    @RequestMapping({ "/4/users/{uid}.*", "/4/users.*" })
    public void outputUsers(HttpServletRequest request, HttpServletResponse response) throws IOException {
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
    
    // If user posts to this endpoint with the oauth token then they are accepting the 
    // terms and conditions
    @RequestMapping(value = "/4/users/{uid}/eula/accept.*", method = RequestMethod.POST)
    public void userAcceptsLicense(HttpServletRequest request, 
            HttpServletResponse response,
            @PathVariable String uid) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Id userId = Id.valueOf(idCodec.decode(uid));
            User editingUser = userFetcher.userFor(request).get();
            // if not own profile then need to be admin
            if (!editingUser.is(Role.ADMIN) && !editingUser.getId().equals(userId)) {
                throw new ResourceForbiddenException();
            }
            Optional<User> existing = userStore.userForId(userId);
            if (!existing.isPresent()) {
                throw new NotFoundException(userId);
            }
            User modified = existing.get().copy().withLicenseAccepted(clock.now()).build();
            UserAwareQueryResult<User> queryResult = UserAwareQueryResult.singleResult(modified, UserAwareQueryContext.standard(request));
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
            HttpServletResponse response,
            @PathVariable String uid) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Id userId = Id.valueOf(idCodec.decode(uid));
            User editingUser = userFetcher.userFor(request).get();
            // if not own profile then need to be admin
            if (!editingUser.is(Role.ADMIN) && !editingUser.getId().equals(userId)) {
                throw new ResourceForbiddenException();
            }
            Optional<User> existing = userStore.userForId(userId);
            if (existing.isPresent()) {
                User posted = deserialize(new InputStreamReader(request.getInputStream()), User.class);
                // Only admins can change the role for a user
                // if editing user is not an admin reject 
                if (!editingUser.is(Role.ADMIN) && isUserRoleChanged(posted, existing.get())) {
                    throw new InsufficientPrivilegeException("You do not have permission to change the user role");
                }
                User modified = updateProfileFields(posted, existing.get(), editingUser);
                userStore.store(modified);
                UserAwareQueryResult<User> queryResult = UserAwareQueryResult.singleResult(modified, UserAwareQueryContext.standard(request));
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

    @RequestMapping(value = "/4/users/deactivate/{uid}.*", method = RequestMethod.POST)
    public void deactivateUser(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String uid) throws IOException {
        ResponseWriter writer = null;
        try {
            User editingUser = userFetcher.userFor(request).get();
            if (!editingUser.is(Role.ADMIN)) {
                throw new ResourceForbiddenException();
            }
            Id userId = Id.valueOf(idCodec.decode(uid));
            Optional<User> user = userStore.userForId(userId);
            if (user.isPresent()) {
                UserRef userRef = user.get().getUserRef();
                Credentials credentialsMaybe = credentialsStore.find(userRef).requireValue();
                Credentials credentials = credentialsMaybe;
                credentialsStore.expired(credentials.authToken());
            } else {
                throw new NotFoundException(userId);
            }
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
            }
    }
    
    private boolean isUserRoleChanged(User posted, User existing) {
        return !posted.getRole().equals(existing.getRole());
    }
    
    /**
     * Only allow certain fields to be updated
     */
    private User updateProfileFields(User posted, User existing, User editingUser) {
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
