package org.atlasapi.application;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParserNoAuth;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryParser;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Optional;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class SourcesController {

    private final UserAwareQueryParser<Publisher> queryParser;
    private final StandardUserAwareQueryParserNoAuth<Publisher> queryParserNoAuth;
    private final UserAwareQueryExecutor<Publisher> queryExecutor;
    private final UserAwareQueryResultWriter<Publisher> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final NumberToShortStringCodec idCodec;
    private final SourceIdCodec sourceIdCodec;
    private final ApplicationStore applicationStore;
    private final UserFetcher userFetcher;
    private final NoAuthUserFetcher userFetcherNoAuth;

    public SourcesController(UserAwareQueryParser<Publisher> queryParser,
            StandardUserAwareQueryParserNoAuth<Publisher> queryParserNoAuth,
            UserAwareQueryExecutor<Publisher> queryExecutor,
            UserAwareQueryResultWriter<Publisher> resultWriter,
            NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec,
            ApplicationStore applicationStore,
            UserFetcher userFetcher,
            NoAuthUserFetcher userFetcherNoAuth) {
        this.queryParser = queryParser;
        this.queryParserNoAuth = queryParserNoAuth;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.idCodec = idCodec;
        this.sourceIdCodec = sourceIdCodec;
        this.applicationStore = applicationStore;
        this.userFetcher = userFetcher;
        this.userFetcherNoAuth = userFetcherNoAuth;
    }

    /**
     * POST /4/sources/:sourceId/applications Updates permission for a source. Post with app id and
     * permission (read/write) required Params: "id": "abc", "permission": "read"
     *
     * @throws NotFoundException
     * @throws ResourceForbiddenException if user does not manage source
     */
    @RequestMapping(value = "/4/sources/{sourceId}/applications", method = RequestMethod.POST)
    public void writeSourceForApplication(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId,
            @RequestParam String id,
            @RequestParam String permission) throws IOException {
        writeSourceForApplicationInternal(request, response, sourceId, id, permission, userFetcher);
    }

    @RequestMapping(value = "/4/admin/sources/{sourceId}/applications", method = RequestMethod.POST)
    public void writeSourceForApplicationNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId,
            @RequestParam String id,
            @RequestParam String permission) throws IOException {
        writeSourceForApplicationInternal(request, response, sourceId, id, permission, userFetcherNoAuth);
    }

    private void writeSourceForApplicationInternal(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String sourceId, @RequestParam String id,
            @RequestParam String permission, UserFetcher userFetcher) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Optional<Publisher> source = sourceIdCodec.decode(sourceId);
            // Only people with admin permission on source can use this endpoint
            if (source.isPresent()) {
                if (!userMangesSource(source.get(), request, userFetcher)) {
                    throw new ResourceForbiddenException();
                }
                Id applicationId = Id.valueOf(idCodec.decode(id));
                Permission permissionType = Permission.valueOf(permission.toUpperCase());
                Application existing = applicationStore.applicationFor(applicationId).get();
                Application modified = null;
                if (permissionType.equals(Permission.READ)) {
                    modified = existing.copyWithSourceEnabled(source.get());
                } else if (permissionType.equals(Permission.WRITE)) {
                    modified = existing.copyWithAddedWritingSource(source.get());
                }
                if (modified != null) {
                    applicationStore.updateApplication(modified);
                }
            } else {
                throw new NotFoundException(null);
            }
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }


    /**
     * DELETE /4/sources/:sourceId/applications Removes a permission (read/write) from an app on a
     * source. Post with app id and permission needed.
     *
     * @throws QueryExecutionException
     * @throws ResourceForbiddenException
     */
    @RequestMapping(value = "/4/sources/{sourceId}/applications", method = RequestMethod.DELETE)
    public void deleteSourceForApplication(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId,
            @RequestParam String id,
            @RequestParam String permission) throws IOException {
        deleteSourceForApplicationInternal(request, response, sourceId, id, permission, userFetcher);
    }

    @RequestMapping(value = "/4/admin/sources/{sourceId}/applications", method = RequestMethod.DELETE)
    public void deleteSourceForApplicationNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId,
            @RequestParam String id,
            @RequestParam String permission) throws IOException {
        deleteSourceForApplicationInternal(
                request,
                response,
                sourceId,
                id,
                permission,
                userFetcherNoAuth
        );
    }

    private void deleteSourceForApplicationInternal(HttpServletRequest request,
            HttpServletResponse response, String sourceId, String id,
            String permission, UserFetcher userFetcher) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Optional<Publisher> source = sourceIdCodec.decode(sourceId);
            if (source.isPresent()) {
                // Only people with admin permission on source can use this endpoint
                if (!userMangesSource(source.get(), request, userFetcher)) {
                    throw new ResourceForbiddenException();
                }
                Id applicationId = Id.valueOf(idCodec.decode(id));
                Permission permissionType = Permission.valueOf(permission.toUpperCase());
                Application existing = applicationStore.applicationFor(applicationId).get();
                Application modified = null;
                if (permissionType.equals(Permission.READ)) {
                    modified = existing.copyWithSourceDisabled(source.get());
                } else if (permissionType.equals(Permission.WRITE)) {
                    modified = existing.copyWithRemovedWritingSource(source.get());
                }
                if (modified != null) {
                    applicationStore.updateApplication(modified);
                }
            } else {
                throw new QueryExecutionException("No source with id " + sourceId);
            }
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }


    /**
     * POST /4/sources/:sourceId/applications/readers/:appId/state Changes state of app for source,
     * e.g. "available", "requested". Params: "state": "available"
     *
     * @throws ResourceForbiddenException
     * @throws Exception
     */
    @RequestMapping(value = "/4/sources/{sourceId}/applications/readers/{id}/state",
            method = RequestMethod.POST)
    public void changeSourceStateForApplication(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId,
            @PathVariable String id,
            @RequestParam String state) throws IOException {
        changeSourceStateForApplicationInternal(request, response, sourceId, id, state, userFetcher);
    }

    @RequestMapping(value = "/4/admin/sources/{sourceId}/applications/readers/{id}/state",
            method = RequestMethod.POST)
    public void changeSourceStateForApplicationNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sourceId,
            @PathVariable String id,
            @RequestParam String state) throws IOException {
        changeSourceStateForApplicationInternal(
                request,
                response,
                sourceId,
                id,
                state,
                userFetcherNoAuth
        );
    }

    private void changeSourceStateForApplicationInternal(HttpServletRequest request,
            HttpServletResponse response, String sourceId, String id,
            String state, UserFetcher userFetcher) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Optional<Publisher> source = sourceIdCodec.decode(sourceId);
            if (source.isPresent()) {
                // Only people with admin permission on source can use this endpoint
                if (!userMangesSource(source.get(), request, userFetcher)) {
                    throw new ResourceForbiddenException();
                }
                Id applicationId = Id.valueOf(idCodec.decode(id));
                SourceState requestedState = SourceState.valueOf(state.toUpperCase());
                Application existing = applicationStore.applicationFor(applicationId).get();
                applicationStore.updateApplication(
                        existing.copyWithReadSourceState(source.get(), requestedState));
            } else {
                throw new QueryExecutionException("No source with id " + sourceId);
            }
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }


    @RequestMapping({ "/4/sources/{sid}.*", "/4/sources.*" })
    public void listSources(HttpServletRequest request,
            HttpServletResponse response)
            throws QueryParseException, QueryExecutionException, IOException {
        listSourcesInternal(request, response, queryParser);
    }

    @RequestMapping({ "/4/admin/sources/{sid}.*", "/4/sources.*" })
    public void listSourcesNoAuth(HttpServletRequest request,
            HttpServletResponse response)
            throws QueryParseException, QueryExecutionException, IOException {
        listSourcesInternal(request, response, queryParserNoAuth);
    }

    private void listSourcesInternal(HttpServletRequest request, HttpServletResponse response,
            UserAwareQueryParser<Publisher> queryParser)
            throws IOException, UnsupportedFormatException, NotAcceptableException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
            UserAwareQuery<Publisher> sourcesQuery = queryParser.parse(request);
            UserAwareQueryResult<Publisher> queryResult = queryExecutor.execute(sourcesQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }


    private boolean userMangesSource(Publisher source, HttpServletRequest request,
            UserFetcher userFetcher) {
        Optional<User> user = userFetcher.userFor(request);
        if (!user.isPresent()) {
            return false;
        } else {
            return user.get().is(Role.ADMIN) || user.get().getSources().contains(source);
        }
    }
}
