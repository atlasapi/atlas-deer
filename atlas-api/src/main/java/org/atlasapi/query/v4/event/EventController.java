package org.atlasapi.query.v4.event;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.event.Event;
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

/**
 * An endpoint for serving pieces of Event. Event can be fetched by unique ID.
 */

@ProducesType(type = Event.class)
@Controller
@RequestMapping("/4/events")
public class EventController {

    private static final Logger LOG = LoggerFactory.getLogger(EventController.class);
    private final QueryParser<Event> queryParser;
    private final QueryExecutor<Event> queryExecutor;
    private final QueryResultWriter<Event> queryResultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public EventController(QueryParser<Event> queryParser,
            QueryExecutor<Event> queryExecutor,
            QueryResultWriter<Event> queryResultWriter) {
        this.queryParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.queryResultWriter = queryResultWriter;
    }

    @RequestMapping({ "/{id}\\.[a-z]+", "/{id}" })
    public void fetchEvent(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {

            writer = writerResolver.writerFor(request, response);
            Query<Event> query = queryParser.parse(request);
            QueryResult<Event> result = queryExecutor.execute(query);
            queryResultWriter.write(result, writer);

        } catch (Exception e) {
            LOG.error("Request Exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);

        }
    }

}
