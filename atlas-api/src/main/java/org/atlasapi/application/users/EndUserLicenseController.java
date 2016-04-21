package org.atlasapi.application.users;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.EndUserLicense;
import org.atlasapi.application.EndUserLicenseStore;
import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.entity.Id;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class EndUserLicenseController {

    private static Logger log = LoggerFactory.getLogger(UsersController.class);

    public static final Id LICENSE_ID = Id.valueOf(100);
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final QueryResultWriter<EndUserLicense> resultWriter;
    private final ModelReader reader;
    private final EndUserLicenseStore licenseStore;
    private final UserFetcher userFetcher;
    private final NoAuthUserFetcher userFetcherNoAuth;

    public EndUserLicenseController(QueryResultWriter<EndUserLicense> resultWriter,
            ModelReader reader, EndUserLicenseStore licenseStore, UserFetcher userFetcher, NoAuthUserFetcher userFetcherNoAuth) {
        super();
        this.resultWriter = resultWriter;
        this.reader = reader;
        this.licenseStore = licenseStore;
        this.userFetcher = userFetcher;
        this.userFetcherNoAuth = userFetcherNoAuth;
    }

    @RequestMapping({ "/4/eula.*" })
    public void outputLicense(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            EndUserLicense license = licenseStore.getById(LICENSE_ID);
            QueryResult<EndUserLicense> queryResult = QueryResult.singleResult(
                    license,
                    QueryContext.standard(request)
            );
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping({ "/4/admin/eula.*" })
    public void outputLicenseNoAuth(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            EndUserLicense license = licenseStore.getById(LICENSE_ID);
            QueryResult<EndUserLicense> queryResult = QueryResult.singleResult(
                    license,
                    QueryContext.standard(request)
            );
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/eula.*", method = RequestMethod.POST)
    public void updatelicense(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            User editingUser = userFetcher.userFor(request).get();
            if (!editingUser.is(Role.ADMIN)) {
                throw new ResourceForbiddenException();
            }

            writer = writerResolver.writerFor(request, response);
            EndUserLicense posted = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    EndUserLicense.class
            );
            // set to the EULA id
            licenseStore.store(posted.copy().withId(LICENSE_ID).build());
            QueryResult<EndUserLicense> queryResult = QueryResult.singleResult(
                    posted,
                    QueryContext.standard(request)
            );
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/admin/eula.*", method = RequestMethod.POST)
    public void updatelicenseNoAuth(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            Set<User> editingUserAccounts = userFetcherNoAuth.userFor(request);
            if (!editingUserAccounts
                    .stream()
                    .filter(user -> user.is(Role.ADMIN))
                    .findAny()
                    .isPresent()) {
                throw new ResourceForbiddenException();
            }

            writer = writerResolver.writerFor(request, response);
            EndUserLicense posted = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    EndUserLicense.class
            );
            // set to the EULA id
            licenseStore.store(posted.copy().withId(LICENSE_ID).build());
            QueryResult<EndUserLicense> queryResult = QueryResult.singleResult(
                    posted,
                    QueryContext.standard(request)
            );
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private <T> T deserialize(Reader input, Class<T> cls) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), cls);
    }
}
