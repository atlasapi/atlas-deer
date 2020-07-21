package org.atlasapi.query.v4.topic;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.output.ContextualResultWriter;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.ContextualQuery;
import org.atlasapi.query.common.ContextualQueryExecutor;
import org.atlasapi.query.common.ContextualQueryParser;
import org.atlasapi.query.common.ContextualQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.topic.Topic;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class TopicContentController {

    private static Logger log = LoggerFactory.getLogger(TopicContentController.class);

    private final ContextualQueryParser<Topic, Content> parser;
    private final ContextualQueryExecutor<Topic, Content> queryExecutor;
    private final ContextualResultWriter<Topic, Content> resultWriter;

    private ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public TopicContentController(ContextualQueryParser<Topic, Content> parser,
            ContextualQueryExecutor<Topic, Content> queryExecutor,
            ContextualResultWriter<Topic, Content> resultWriter) {
        this.parser = checkNotNull(parser);
        this.queryExecutor = checkNotNull(queryExecutor);
        this.resultWriter = checkNotNull(resultWriter);
    }

    @RequestMapping({ "/4/topics/{id}/content\\.[a-z]+", "/4/topics/{id}/content" })
    public void writeSingleTopic(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            ContextualQuery<Topic, Content> query = parser.parse(request);
            ContextualQueryResult<Topic, Content> result = queryExecutor.execute(query);
            resultWriter.write(result, writer);
        } catch (QueryExecutionException qee) {
            log.error("Query execution exception " + request.getRequestURI(), qee.getCause());
            handleException(request, response, writer, qee);
        } catch (Exception e) {
            log.error(
                    "Request exception " + request.getRequestURI(),
                    MoreObjects.firstNonNull(e.getCause(), e)
            );
            handleException(request, response, writer, e);
        }
    }

    private void handleException(HttpServletRequest request, HttpServletResponse response,
            ResponseWriter writer, Exception e) throws IOException {
        ErrorSummary summary = ErrorSummary.forException(e);
        new ErrorResultWriter().write(summary, writer, request, response);
    }

}
