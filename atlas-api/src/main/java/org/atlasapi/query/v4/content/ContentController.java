package org.atlasapi.query.v4.content;

import java.io.IOException;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.content.Content;
import org.atlasapi.input.ModelReader;
import org.atlasapi.messaging.ContentMessage;
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
import org.atlasapi.util.ClientModelConverter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;

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

    private final ModelReader modelReader;
    private final ClientModelConverter converter;
    private final MessageSender<ContentMessage> messageSender;

    public ContentController(QueryParser<Content> queryParser,
            QueryExecutor<Content> queryExecutor, QueryResultWriter<Content> resultWriter,
            ModelReader modelReader, ClientModelConverter converter,
            MessageSender<ContentMessage> messageSender) {
        this.requestParser = queryParser;
        this.queryExecutor = queryExecutor;
        this.resultWriter = resultWriter;
        this.modelReader = modelReader;
        this.converter = converter;
        this.messageSender = messageSender;
    }

    @RequestMapping(value = { "/{id}.*", "/{id}", ".*", "" }, method = RequestMethod.GET)
    public void fetchContent(HttpServletRequest request, HttpServletResponse response, @RequestParam(value = "order_by", required = false) String orderBy)
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

    @RequestMapping(value = { "" }, method = RequestMethod.POST)
    public void addContent(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        try {
            // TODO: seems like auth is done by the requestParser, should I handle that?
            org.atlasapi.deer.client.model.types.Content requestContent =
                    modelReader.read(request.getReader(), org.atlasapi.deer.client.model.types.Content.class);
            Content content = converter.convert(requestContent);

            messageSender.sendMessage(
                    new ContentMessage(
                            UUID.randomUUID().toString(), Timestamp.of(DateTime.now()), content));

            response.setStatus(HttpStatus.ACCEPTED.value());
        } catch (Exception e) {
            log.error("Request exception {}", request.getRequestURI(), e);
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }
}
