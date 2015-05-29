package org.atlasapi.query.v4.content;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.content.QueryOrdering;
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

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * An endpoint for serving pieces of Content. Content can be fetched either by
 * unique ID or by adding filter parameters to the endpoint. 
 * 
 */
@ProducesType(type=Content.class)
@Controller
@RequestMapping("/4/content")
public class ContentController {

    private static Logger log = LoggerFactory.getLogger(ContentController.class);

    private final QueryParser<Content> requestParser;
    private final QueryExecutor<Content> queryExecutor;
    private final QueryResultWriter<Content> resultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public ContentController(QueryParser<Content> queryParser,
        QueryExecutor<Content> queryExecutor, QueryResultWriter<Content> resultWriter) {
        this.requestParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "/{id}.*", "/{id}", ".*", "" })
    public void fetchContent(HttpServletRequest request, HttpServletResponse response, @RequestParam("order_by") String orderBy)
        throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<Content> contentQuery = requestParser.parse(request);
            QueryResult<Content> queryResult = queryExecutor.execute(contentQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
