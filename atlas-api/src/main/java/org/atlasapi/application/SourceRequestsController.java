package org.atlasapi.application;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
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
public class SourceRequestsController {

    private final UserAwareQueryParser<SourceRequest> queryParser;
    private final UserAwareQueryExecutor<SourceRequest> queryExecutor;
    private final UserAwareQueryResultWriter<SourceRequest> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final SourceRequestManager sourceRequestManager;
    private final NumberToShortStringCodec idCodec;
    private final SourceIdCodec sourceIdCodec;
    private final UserFetcher userFetcher;
    private final StandardUserAwareQueryParserNoAuth<SourceRequest> queryParserNoAuth;
    private final NoAuthUserFetcher userFetcherNoAuth;

    public SourceRequestsController(UserAwareQueryParser<SourceRequest> queryParser,
            StandardUserAwareQueryParserNoAuth<SourceRequest> queryParserNoAuth,
            NoAuthUserFetcher userFetcherNoAuth,
            UserAwareQueryExecutor<SourceRequest> queryExecutor,
            UserAwareQueryResultWriter<SourceRequest> resultWriter,
            SourceRequestManager sourceRequestManager,
            NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec,
            UserFetcher userFetcher) {
        this.queryParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.sourceRequestManager = sourceRequestManager;
        this.idCodec = idCodec;
        this.sourceIdCodec = sourceIdCodec;
        this.userFetcher = userFetcher;
        this.queryParserNoAuth = queryParserNoAuth;
        this.userFetcherNoAuth = userFetcherNoAuth;
    }

    @RequestMapping(value = { "/4/requests.*", "/4/requests/{id}.*" }, method = RequestMethod.GET)
    public void listSourceRequests(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        listSourceRequestsInternal(request, response, queryParser);

    }

    @RequestMapping(value = { "/4/admin/requests.*", "/4/admin/requests/{id}.*" }, method = RequestMethod.GET)
    public void listSourceRequestsNoAuth(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        listSourceRequestsInternal(request, response, queryParserNoAuth);

    }

    private void listSourceRequestsInternal(HttpServletRequest request,
            HttpServletResponse response, UserAwareQueryParser<SourceRequest> queryParser) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            UserAwareQuery<SourceRequest> sourcesQuery = queryParser.parse(request);
            UserAwareQueryResult<SourceRequest> queryResult = queryExecutor.execute(sourcesQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping(value = "/4/sources/{sid}/requests", method = RequestMethod.POST)
    public void storeSourceRequest(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sid,
            @RequestParam String appId,
            @RequestParam String appUrl,
            @RequestParam String reason,
            @RequestParam String usageType,
            @RequestParam String licenseAccepted) throws IOException {

        storeSourceRequestInternal(
                request,
                response,
                sid,
                appId,
                appUrl,
                reason,
                usageType,
                licenseAccepted, userFetcher
        );
    }

    @RequestMapping(value = "/4/admin/sources/{sid}/requests", method = RequestMethod.POST)
    public void storeSourceRequestNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String sid,
            @RequestParam String appId,
            @RequestParam String appUrl,
            @RequestParam String reason,
            @RequestParam String usageType,
            @RequestParam String licenseAccepted) throws IOException {

        storeSourceRequestInternal(
                request,
                response,
                sid,
                appId,
                appUrl,
                reason,
                usageType,
                licenseAccepted,
                userFetcherNoAuth
        );
    }
    private void storeSourceRequestInternal(HttpServletRequest request,
            HttpServletResponse response, String sid, String appId,
            String appUrl, String reason,
            String usageType, String licenseAccepted,
            UserFetcher userFetcher)
            throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Optional<Publisher> source = sourceIdCodec.decode(sid);
            if (!source.isPresent()) {
                throw new NotFoundException(null);
            }
            Id applicationId = Id.valueOf(idCodec.decode(appId));
            UsageType usageTypeRequested = UsageType.valueOf(usageType.toUpperCase());
            User user = userFetcher.userFor(request).get();
            sourceRequestManager.createOrUpdateRequest(source.get(), usageTypeRequested,
                    applicationId, appUrl, user.getEmail(), reason, Boolean.valueOf(licenseAccepted)
            );
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

    @RequestMapping(value = "/4/requests/{rid}/approve", method = RequestMethod.POST)
    public void storeSourceRequest(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String rid) throws IOException {
        storeSourceRequestInternal(request, response, rid, userFetcher);
    }

    @RequestMapping(value = "/4/admin/requests/{rid}/approve", method = RequestMethod.POST)
    public void storeSourceRequestNoAuth(HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable String rid) throws IOException {
        storeSourceRequestInternal(request, response, rid, userFetcherNoAuth);
    }

    private void storeSourceRequestInternal(HttpServletRequest request,
            HttpServletResponse response, @PathVariable String rid, UserFetcher userFetcher) throws IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            Id requestId = Id.valueOf(idCodec.decode(rid));
            sourceRequestManager.approveSourceRequest(
                    requestId,
                    userFetcher.userFor(request).get()
            );
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, null, request, response);
        }
    }

}
