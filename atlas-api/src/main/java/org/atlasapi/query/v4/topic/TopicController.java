package org.atlasapi.query.v4.topic;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.atlasapi.topic.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@ProducesType(type = Topic.class)
@Controller
@RequestMapping("/4/topics")
public class TopicController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private final QueryParser<Topic> requestParser;
    private final QueryExecutor<Topic> queryExecutor;
    private final QueryResultWriter<Topic> resultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public TopicController(QueryParser<Topic> queryParser,
            QueryExecutor<Topic> queryExecutor, QueryResultWriter<Topic> resultWriter) {
        this.requestParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "/{id}\\.[a-z]+", "/{id}", "\\.[a-z]+", "" })
    public void writeSingleTopic(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<Topic> topicQuery = requestParser.parse(request);
            QueryResult<Topic> queryResult = queryExecutor.execute(topicQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

}
