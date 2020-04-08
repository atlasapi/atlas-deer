package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.ApiKeyApplicationFetcher;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Identified;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.coercers.InstantRangeCoercer;
import org.atlasapi.query.common.coercers.IntegerRangeCoercer;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.v2.ParameterChecker;
import org.atlasapi.query.v4.topic.TopicController;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.search.Range;
import com.metabroadcast.sherlock.client.search.SearchHelper;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class SearchController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    private static final char VALUE_SEPARATOR = ',';

    private static final String QUERY_PARAM = "q";
    private static final String YEAR_PARAM = "filter.year";
    private static final String TYPE_PARAM = "filter.type";
    private static final String PUBLISHER_PARAM = "filter.publisher";
    private static final String SCHEDULE_UPCOMING_PARAM = "filter.schedule.upcoming";
    private static final String SCHEDULE_TIME_PARAM = "filter.schedule.time";
    private static final String SCHEDULE_CHANNEL_PARAM = "filterOption.schedule.channel";
    private static final String SCHEDULE_CHANNEL_GROUP_PARAM = "filterOption.schedule.channelGroup";
    private static final String ON_DEMAND_AVAILABLE_PARAM = "filter.ondemand.available";

    private final ContentResolvingSearcher searcher;
    private final QueryResultWriter<Content> resultWriter;

    private final InstantRangeCoercer instantRangeCoercer = InstantRangeCoercer.create();
    private final IntegerRangeCoercer integerRangeCoercer = IntegerRangeCoercer.create();

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    private final ParameterChecker paramChecker = new ParameterChecker(ImmutableSet.of(
            ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER,
            Selection.LIMIT_REQUEST_PARAM,
            Selection.START_INDEX_REQUEST_PARAM,
            QUERY_PARAM,
            YEAR_PARAM,
            TYPE_PARAM,
            PUBLISHER_PARAM,
            SCHEDULE_UPCOMING_PARAM,
            SCHEDULE_TIME_PARAM,
            SCHEDULE_CHANNEL_PARAM,
            SCHEDULE_CHANNEL_GROUP_PARAM,
            ON_DEMAND_AVAILABLE_PARAM
    ));

    public SearchController(
            ContentResolvingSearcher searcher,
            QueryResultWriter<Content> resultWriter
    ) {
        this.searcher = searcher;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "/5/search\\.[a-z]+", "/5/search" })
    public void search(
            @RequestParam(QUERY_PARAM) String query,
            @RequestParam(value = YEAR_PARAM, required = false) String yearParam,
            @RequestParam(value = TYPE_PARAM, required = false) String typeParam,
            @RequestParam(value = PUBLISHER_PARAM, required = false) String publisherParam,
            @RequestParam(value = SCHEDULE_UPCOMING_PARAM, required = false) Boolean scheduleUpcomingParam,
            @RequestParam(value = SCHEDULE_TIME_PARAM, required = false) String scheduleTimeParam,
            @RequestParam(value = SCHEDULE_CHANNEL_PARAM, required = false) String scheduleChannelParam,
            @RequestParam(value = SCHEDULE_CHANNEL_GROUP_PARAM, required = false) String scheduleChannelGroupParam,
            @RequestParam(value = ON_DEMAND_AVAILABLE_PARAM, required = false) Boolean onDemandAvailableParam,
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            paramChecker.checkParameters(request);

            if (Strings.isNullOrEmpty(query)) {
                throw new IllegalArgumentException("You must specify a query parameter");
            }

            Selection selection = Selection.builder().build(request);
            if (!selection.hasLimit()) {
                throw new IllegalArgumentException("You must specify a limit parameter");
            }

            SearchHelper.Builder searchQuery = SearchHelper.getDefaultQuerySearcher(query);

            List<Range<Integer>> years = integerRangeCoercer.apply(distinctSplit(yearParam));
            for (Range<Integer> year : years) {
                searchQuery.addFilter(CONTENT_MAPPING.getYear(), year);
            }

            List<String> types = distinctSplit(typeParam);
            for (String type : types) {
                searchQuery.addFilter(CONTENT_MAPPING.getType(), type);
            }

            List<String> publishers = distinctSplit(publisherParam);
            for (String publisher : publishers) {
                searchQuery.addFilter(CONTENT_MAPPING.getSource().getKey(), publisher);
            }

            if (scheduleUpcomingParam != null && scheduleUpcomingParam) {
                searchQuery.addFilter(
                        CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                        Range.from(Instant.now())
                );
            }

            List<Range<Instant>> scheduleTimes = instantRangeCoercer.apply(distinctSplit(scheduleTimeParam));
            for (Range<Instant> scheduleTime : scheduleTimes) {
                searchQuery.addFilter(
                        CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                        scheduleTime
                );
            }

            List<String> scheduleChannels = distinctSplit(scheduleChannelParam);
            for (String scheduleChannel : scheduleChannels) {
                searchQuery.addFilter(
                        CONTENT_MAPPING.getBroadcasts().getBroadcastOn(),
                        scheduleChannel
                );
            }

            // TODO
//            List<String> scheduleChannelGroups = extractValuesFromParameter(scheduleChannelGroupParam);
//            for (String scheduleChannelGroup : scheduleChannelGroups) {
//                searchHelper.addFilter(
//                        CONTENT_MAPPING.getBroadcasts().getChannelGroup(),
//                        scheduleChannelGroup
//                );
//            }

            if (onDemandAvailableParam != null) {
                searchQuery.addFilter(
                        CONTENT_MAPPING.getLocations().getAvailable(),
                        onDemandAvailableParam
                );
            }

            List<Identified> content = searcher.search(searchQuery.build());
            resultWriter.write(QueryResult.listResult(
                    Iterables.filter(content, Content.class),
                    QueryContext.standard(request),
                    Long.valueOf(content.size())
            ), writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private List<String> distinctSplit(String parameter) {
        if (parameter == null) {
            return ImmutableList.of();
        } else {
            return Arrays.stream(parameter.split(String.valueOf(VALUE_SEPARATOR), -1))
                    .distinct()
                    .collect(Collectors.toList());
        }
    }
}
