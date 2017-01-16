package org.atlasapi.application;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.QueryParseException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.QueryExecutionException;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class SourcesController {

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public SourcesController() {
    }

    @RequestMapping({ "/4/sources/{sid}.*", "/4/sources.*" })
    public void listSources(HttpServletRequest request,
            HttpServletResponse response)
            throws QueryParseException, QueryExecutionException, IOException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
//            UserAwareQuery<Publisher> sourcesQuery = queryParser.parse(request);
//            UserAwareQueryResult<Publisher> queryResult = queryExecutor.execute(sourcesQuery);
//            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    @RequestMapping({ "/4/admin/sources/{sid}.*", "/4/admin/sources.*" })
    //TODO: make a simplified version of this just gets all the publishers and prints them to you. Want it under /system/sources, not /4/
    public void listSourcesNoAuth(HttpServletRequest request,
            HttpServletResponse response)
            throws QueryParseException, QueryExecutionException, IOException {
        ResponseWriter writer = writerResolver.writerFor(request, response);
        try {
//            UserAccountsAwareQuery<Publisher> sourcesQuery = queryParserNoAuth.parse(request);
//            UserAccountsAwareQueryResult<Publisher> queryResult = queryExecutorNoAuth.execute(
//                    sourcesQuery);
//            resultWriterNoAuth.write(queryResult, writer);
        } catch (Exception e) {
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
