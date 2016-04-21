package org.atlasapi.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParserNoAuth;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class SourceLicenseController {

    private static Logger log = LoggerFactory.getLogger(SourceLicenseController.class);
    private final UserAwareQueryParser<SourceLicense> queryParser;
    private final StandardUserAwareQueryParserNoAuth<SourceLicense> queryParserNoAuth;
    private final UserAwareQueryExecutor<SourceLicense> queryExecutor;
    private final UserAwareQueryResultWriter<SourceLicense> resultWriter;
    private final UserAccountsAwareQueryExecutor<SourceLicense> queryExecutorNoAuth;
    private final UserAccountsAwareQueryResultWriter<SourceLicense> resultWriterNoAuth;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final ModelReader reader;
    private final UserFetcher userFetcher;
    private final NoAuthUserFetcher userFetcherNoAuth;
    private final SourceLicenseStore store;

    public SourceLicenseController(UserAwareQueryParser<SourceLicense> queryParser,
            StandardUserAwareQueryParserNoAuth<SourceLicense> queryParserNoAuth,
            UserAwareQueryExecutor<SourceLicense> queryExecutor,
            UserAwareQueryResultWriter<SourceLicense> resultWriter,
            UserAccountsAwareQueryExecutor<SourceLicense> queryExecutorNoAuth,
            UserAccountsAwareQueryResultWriter<SourceLicense> resultWriterNoAuth,
            ModelReader reader,
            UserFetcher userFetcher,
            NoAuthUserFetcher userFetcherNoAuth,
            SourceLicenseStore store) {
        super();
        this.queryParser = queryParser;
        this.queryParserNoAuth = queryParserNoAuth;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.reader = reader;
        this.userFetcher = userFetcher;
        this.userFetcherNoAuth = userFetcherNoAuth;
        this.store = store;
        this.queryExecutorNoAuth = queryExecutorNoAuth;
        this.resultWriterNoAuth = resultWriterNoAuth;
    }

    @RequestMapping({ "/4/source_licenses/{sid}.*", "/4/source_licenses.*" })
    public void listSources(HttpServletRequest request,
            HttpServletResponse response)
            throws QueryParseException, QueryExecutionException, IOException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            UserAwareQuery<SourceLicense> sourcesQuery = queryParser.parse(request);
            UserAwareQueryResult<SourceLicense> queryResult = queryExecutor.execute(sourcesQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping({ "/4/admin/source_licenses/{sid}.*", "/4/admin/source_licenses.*" })
    public void listSourcesNoAuth(HttpServletRequest request,
            HttpServletResponse response)
            throws QueryParseException, QueryExecutionException, IOException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            UserAccountsAwareQuery<SourceLicense> sourcesQuery = queryParserNoAuth.parse(request);
            UserAccountsAwareQueryResult<SourceLicense> queryResult = queryExecutorNoAuth.execute(sourcesQuery);
            resultWriterNoAuth.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/license.*", method = RequestMethod.POST)
    public void writeLicense(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            User user = userFetcher.userFor(request).get();
            if (!user.is(Role.ADMIN)) {
                throw new ResourceForbiddenException();
            }

            SourceLicense license = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    SourceLicense.class
            );
            store.store(license);
            UserAwareQueryResult<SourceLicense> queryResult = UserAwareQueryResult.singleResult(
                    license,
                    UserAwareQueryContext.standard(request)
            );
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }


    @RequestMapping(value = "/4/admin/license.*", method = RequestMethod.POST)
    public void writeLicenseNoAuth(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Set<User> userAccounts = userFetcherNoAuth.userFor(request);
            if (!userAccounts.stream().filter(user->user.is(Role.ADMIN)).findAny().isPresent()) {
                throw new ResourceForbiddenException();
            }

            SourceLicense license = deserialize(
                    new InputStreamReader(request.getInputStream()),
                    SourceLicense.class
            );
            store.store(license);
            UserAccountsAwareQueryResult<SourceLicense> queryResult = UserAccountsAwareQueryResult.singleResult(
                    license,
                    UserAccountsAwareQueryContext.standard(request)
            );
            resultWriterNoAuth.write(queryResult, writer);
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
