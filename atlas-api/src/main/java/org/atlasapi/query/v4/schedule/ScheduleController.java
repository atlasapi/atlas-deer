package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.meta.annotations.ProducesType;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.annotation.ContextualAnnotationsExtractor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.metabroadcast.common.time.SystemClock;

@ProducesType(type = ChannelSchedule.class)
@Controller
@RequestMapping("/4/schedules")
public class ScheduleController {

    private static Logger log = LoggerFactory.getLogger(ScheduleController.class);

    private static final Duration MAX_REQUEST_DURATION = Duration.standardDays(1);

    private final ScheduleRequestParser requestParser;
    private final ScheduleQueryExecutor queryExecutor;
    private final QueryResultWriter<ChannelSchedule> resultWriter;

    private ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public ScheduleController(ScheduleQueryExecutor queryExecutor,
        ApplicationSourcesFetcher appFetcher,
        QueryResultWriter<ChannelSchedule> resultWriter,
        ContextualAnnotationsExtractor annotationsExtractor) {
        this.requestParser = new ScheduleRequestParser(
            appFetcher,
            MAX_REQUEST_DURATION,
            new SystemClock(), annotationsExtractor
        );
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "", ".*", "/{id}.*", "/{id}" })
    public void getChannelSchedule(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            ScheduleQuery scheduleQuery = requestParser.queryFrom(request);
            QueryResult<ChannelSchedule> queryResult = queryExecutor.execute(scheduleQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

}
