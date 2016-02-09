package org.atlasapi.query.v4.channelgroup;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.channel.ChannelGroup;
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

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
@RequestMapping("/4/channel_groups")
@ProducesType(type = ChannelGroup.class)
public class ChannelGroupController {

    private final QueryParser<ChannelGroup<?>> requestParser;
    private final QueryExecutor<ChannelGroup<?>> queryExecutor;
    private final QueryResultWriter<ChannelGroup<?>> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    private static Logger log = LoggerFactory.getLogger(ChannelGroupController.class);

    public ChannelGroupController(
            QueryParser<ChannelGroup<?>> requestParser,
            QueryExecutor<ChannelGroup<?>> queryExecutor,
            QueryResultWriter<ChannelGroup<?>> resultWriter
    ) {
        this.requestParser = checkNotNull(requestParser);
        this.queryExecutor = checkNotNull(queryExecutor);
        this.resultWriter = checkNotNull(resultWriter);
    }

    @RequestMapping({ "", ".*", "/{id}.*", "/{id}" })
    public void fetchChannelGroup(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<ChannelGroup<?>> channelGroupQuery = requestParser.parse(request);
            QueryResult<ChannelGroup<?>> queryResult = queryExecutor.execute(channelGroupQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
