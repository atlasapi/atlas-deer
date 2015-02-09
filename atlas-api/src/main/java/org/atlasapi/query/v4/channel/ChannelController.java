package org.atlasapi.query.v4.channel;

import org.atlasapi.channel.Channel;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
@RequestMapping("/4/channels")
@ProducesType(type = Channel.class)
public class ChannelController {

    private final QueryParser<Channel> requestParser;
    private final QueryExecutor<Channel> queryExecutor;
    private final QueryResultWriter<Channel> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    private static Logger log = LoggerFactory.getLogger(ChannelController.class);

    public ChannelController(
            QueryParser<Channel> requestParser,
            QueryExecutor<Channel> queryExecutor,
            QueryResultWriter<Channel> resultWriter
    ) {
        this.requestParser = checkNotNull(requestParser);
        this.queryExecutor = checkNotNull(queryExecutor);
        this.resultWriter = checkNotNull(resultWriter);
    }

    @RequestMapping({"", ".*", "/{cid}.*", "/{cid}"})
    public void fetchChannel(HttpServletRequest request, HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Query<Channel> channelQuery = requestParser.parse(request);
            QueryResult<Channel> queryResult = queryExecutor.execute(channelQuery);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
