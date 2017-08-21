package org.atlasapi.query.v4.content;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.meta.annotations.ProducesType;
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

/**
 * An endpoint for serving pieces of Content. Content can be fetched either by unique ID or by
 * adding filter parameters to the endpoint.
 */
@ProducesType(type = Content.class)
@Controller
@RequestMapping("/4/content")
public class ContentController {

    private static final Logger log = LoggerFactory.getLogger(ContentController.class);

    private final QueryParser<ResolvedContent> requestParser;
    private final QueryExecutor<ResolvedContent> queryExecutor;
    private final QueryResultWriter<ResolvedContent> resultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public ContentController(
            QueryParser<ResolvedContent> queryParser,
            QueryExecutor<ResolvedContent> queryExecutor,
            QueryResultWriter<ResolvedContent> resultWriter
    ) {
        this.requestParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "/{id}\\.[a-z]+", "/{id}", "\\.[a-z]+", "" })
    public void fetchContent(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = "order_by", required = false) String orderBy)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<ResolvedContent> contentQuery = requestParser.parse(request);
            QueryResult<ResolvedContent> queryResult = queryExecutor.execute(contentQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            String queryString = request.getQueryString();
            log.error("Request exception " + request.getRequestURI() + (queryString != null
                                                                        ? queryString
                                                                        : ""), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
