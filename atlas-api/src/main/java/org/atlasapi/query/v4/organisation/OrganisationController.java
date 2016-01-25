package org.atlasapi.query.v4.organisation;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.meta.annotations.ProducesType;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryParser;
import org.atlasapi.query.common.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@ProducesType(type=Organisation.class)
@Controller
@RequestMapping("/4/organisations")
public class OrganisationController {

    private static Logger log = LoggerFactory.getLogger(OrganisationController.class);

    private final QueryParser<Organisation> requestParser;
    private final QueryExecutor<Organisation> queryExecutor;
    private final QueryResultWriter<Organisation> resultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public OrganisationController(QueryParser<Organisation> queryParser,
            QueryExecutor<Organisation> queryExecutor, QueryResultWriter<Organisation> resultWriter) {
        this.requestParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({"/{id}.*", "/{id}"})
    public void fetchContent(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<Organisation> contentQuery = requestParser.parse(request);
            QueryResult<Organisation> queryResult = queryExecutor.execute(contentQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
