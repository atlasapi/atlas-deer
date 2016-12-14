package org.atlasapi.query.v4.topic;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.auth.ApplicationFetcher;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.topic.PopularTopicIndex;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;

import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class PopularTopicController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private final DateTimeInQueryParser dateTimeInQueryParser = new DateTimeInQueryParser();
    private final TopicResolver resolver;
    private final PopularTopicIndex index;
    private final QueryResultWriter<Topic> resultWriter;
    private final ApplicationFetcher sourcesFetcher;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public PopularTopicController(TopicResolver resolver, PopularTopicIndex index,
            QueryResultWriter<Topic> resultWriter, ApplicationFetcher configurationFetcher) {
        this.resolver = resolver;
        this.index = index;
        this.resultWriter = resultWriter;
        this.sourcesFetcher = configurationFetcher;
    }

    @RequestMapping({ "/4/topics/popular\\.[a-z]+", "/4/topics/popular" })
    public void popularTopics(@RequestParam(required = true) String from,
            @RequestParam(required = true) String to, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        if (Strings.isNullOrEmpty(from) || Strings.isNullOrEmpty(to)) {
            throw new IllegalArgumentException("Request parameters 'from' and 'to' are required!");
        }
        Selection selection = Selection.builder()
                .withDefaultLimit(Integer.MAX_VALUE)
                .withMaxLimit(Integer.MAX_VALUE)
                .build(request);
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            ApplicationSources sources = sourcesFetcher.applicationFor(request)
                    .or(ApplicationSources.defaults());
            Interval interval = new Interval(
                    dateTimeInQueryParser.parse(from),
                    dateTimeInQueryParser.parse(to)
            );
            ListenableFuture<FluentIterable<Id>> topicIds = index.popularTopics(
                    interval,
                    selection
            );
            resultWriter.write(QueryResult.listResult(
                    resolve(topicIds),
                    QueryContext.create(sources, ActiveAnnotations.standard(), request),
                    Long.valueOf(topicIds.get().size())
            ), writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private Iterable<Topic> resolve(ListenableFuture<FluentIterable<Id>> topicIds)
            throws Exception {
        return Futures.get(Futures.transform(
                topicIds,
                new AsyncFunction<FluentIterable<Id>, Resolved<Topic>>() {

                    @Override
                    public ListenableFuture<Resolved<Topic>> apply(FluentIterable<Id> input) {
                        return resolver.resolveIds(input);
                    }
                }
        ), 60, TimeUnit.SECONDS, Exception.class).getResources();
    }
}
