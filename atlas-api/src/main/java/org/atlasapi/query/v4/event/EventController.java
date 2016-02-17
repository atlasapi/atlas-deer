package org.atlasapi.query.v4.event;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.event.Event;
import org.atlasapi.eventV2.EventV2;
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

@ProducesType(type = EventV2.class)
@Controller
@RequestMapping("/4/events")
public class EventController {

    private static final Logger LOG = LoggerFactory.getLogger(EventController.class);
    private final QueryParser<EventV2> queryParser;
    private final QueryExecutor<EventV2> queryExecutor;
    private final QueryResultWriter<EventV2> queryResultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public EventController(QueryParser<EventV2> queryParser,
            QueryExecutor<EventV2> queryExecutor,
            QueryResultWriter<EventV2> queryResultWriter) {
        this.queryParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.queryResultWriter = queryResultWriter;
    }

    @RequestMapping({ "/{id}.*", "/{id}" })
    public void fetchEvent(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {

            writer = writerResolver.writerFor(request, response);
            Query<EventV2> query = queryParser.parse(request);
            QueryResult<EventV2> result = queryExecutor.execute(query);
            queryResultWriter.write(result, writer);

        } catch (Exception e) {
            LOG.error("Request Exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);

        }
    }

}
