package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.ApiKeyApplicationFetcher;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.application.ApplicationResolutionException;
import org.atlasapi.application.DefaultApplication;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Identified;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.annotation.AnnotationsExtractor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;
import org.atlasapi.query.common.exceptions.InvalidParameterException;
import org.atlasapi.query.common.exceptions.MissingAnnotationException;
import org.atlasapi.query.v2.ParameterChecker;
import org.atlasapi.query.v4.topic.TopicController;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.lucene.index.Term;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import scala.Int;

@Controller
public class SearchController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    private static final char VALUE_SEPARATOR = ',';
    private static final String EXISTS_KEYWORD = "nonNull";
    private static final String NON_EXISTS_KEYWORD = "null";

    private static final String ANNOTATIONS_PARAM = "annotations";
    private static final String QUERY_PARAM = "q";
    private static final String YEAR_PARAM = "filter.year";
    private static final String TYPE_PARAM = "filter.type";
    private static final String PUBLISHER_PARAM = "filter.publisher";
    private static final String SCHEDULE_UPCOMING_PARAM = "filter.schedule.upcoming";
    private static final String SCHEDULE_TIME_PARAM = "filter.schedule.time";
    private static final String SCHEDULE_CHANNEL_PARAM = "filterOption.schedule.channel";
    public static final String SCHEDULE_CHANNEL_GROUP_PARAM = "filterOption.schedule.channelGroup";
    private static final String ON_DEMAND_AVAILABLE_PARAM = "filter.ondemand.available";

    private final ContentResolvingSearcher searcher;
    private final ApplicationFetcher applicationFetcher;
    private final AnnotationsExtractor annotationsExtractor;
    private final Selection.SelectionBuilder selectionBuilder;
    private final QueryResultWriter<Content> resultWriter;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    private final InstantRangeCoercer instantRangeCoercer = InstantRangeCoercer.create();
    private final IntegerRangeCoercer integerRangeCoercer = IntegerRangeCoercer.create();

    private final ParameterChecker paramChecker = new ParameterChecker(ImmutableSet.of(
            ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER,
            Selection.LIMIT_REQUEST_PARAM,
            Selection.START_INDEX_REQUEST_PARAM,
            JsonResponseWriter.CALLBACK,
            ANNOTATIONS_PARAM,
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
            ApplicationFetcher applicationFetcher,
            AnnotationsExtractor annotationsExtractor,
            Selection.SelectionBuilder selectionBuilder,
            QueryResultWriter<Content> resultWriter
    ) {
        this.searcher = searcher;
        this.applicationFetcher = applicationFetcher;
        this.annotationsExtractor = annotationsExtractor;
        this.selectionBuilder = selectionBuilder;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "/5/search\\.[a-z]+", "/5/search" })
    public void search(
            @RequestParam(ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER) String apiKey,
            @RequestParam(value = Selection.LIMIT_REQUEST_PARAM, required = false) Integer limit,
            @RequestParam(value = Selection.START_INDEX_REQUEST_PARAM, required = false) Integer offset,
            @RequestParam(value = QUERY_PARAM, required = false) String query,
            @RequestParam(value = YEAR_PARAM, required = false) String yearParam,
            @RequestParam(value = TYPE_PARAM, required = false) String typeParam,
            @RequestParam(value = PUBLISHER_PARAM, required = false) String publisherParam,
            @RequestParam(value = SCHEDULE_UPCOMING_PARAM, required = false) String scheduleUpcomingParam,
            @RequestParam(value = SCHEDULE_TIME_PARAM, required = false) String scheduleTimeParam,
            @RequestParam(value = SCHEDULE_CHANNEL_PARAM, required = false) String scheduleChannelParam,
            @RequestParam(value = ON_DEMAND_AVAILABLE_PARAM, required = false) String onDemandAvailableParam,
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            paramChecker.checkParameters(request);

            SearchQuery.Builder queryBuilder;
            if (Strings.isNullOrEmpty(query)) {
                 queryBuilder = SearchQuery.builder();
            } else {
                queryBuilder = SearchQuery.getDefaultQuerySearcher(query);
            }

            List<RangeOrTerm<?>> years = integerRangeCoercer.apply(
                    CONTENT_MAPPING.getYear(),
                    distinctSplit(queryBuilder, CONTENT_MAPPING.getYear(), yearParam)
            );
            for (RangeOrTerm<?> year : years) {
                if (year.getRangeOrTermClass() == RangeParameter.class) {
                    queryBuilder.addFilter(year.getRange());
                } else {
                    queryBuilder.addFilter(year.getTerm());
                }
            }

            List<String> types = distinctSplit(queryBuilder, CONTENT_MAPPING.getType(), typeParam);
            for (String type : types) {
                queryBuilder.addFilter(TermParameter.of(CONTENT_MAPPING.getType(), type));
            }

            List<String> publishers = distinctSplit(
                    queryBuilder,
                    CONTENT_MAPPING.getSource().getKey(),
                    publisherParam
            );
            for (String publisher : publishers) {
                queryBuilder.addFilter(
                        TermParameter.of(
                                CONTENT_MAPPING.getSource().getKey(),
                                publisher));
            }

            List<String> scheduleUpcomings = distinctSplit(
                    queryBuilder,
                    CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                    scheduleUpcomingParam
            );
            for (String scheduleUpcoming : scheduleUpcomings) {
                boolean scheduleUpcomingBoolean = Boolean.parseBoolean(scheduleUpcoming);
                if (scheduleUpcomingBoolean) {
                    queryBuilder.addFilter(
                            RangeParameter.from(
                                    CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                                    Instant.now()));
                } else {
                    queryBuilder.addFilter(
                            RangeParameter.to(
                                    CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                                    Instant.now()));
                }
            }

            List<RangeOrTerm<?>> scheduleTimes = instantRangeCoercer.apply(
                    CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                    distinctSplit(
                            queryBuilder,
                            CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                            scheduleTimeParam)
            );
            for (RangeOrTerm<?> scheduleTime : scheduleTimes) {
                if (scheduleTime.getRangeOrTermClass() == RangeParameter.class) {
                    queryBuilder.addFilter(scheduleTime.getRange());
                } else {
                    queryBuilder.addFilter(scheduleTime.getTerm());
                }
            }

            List<String> scheduleChannels = distinctSplit(
                    queryBuilder,
                    CONTENT_MAPPING.getBroadcasts().getBroadcastOn(),
                    scheduleChannelParam
            );
            for (String scheduleChannel : scheduleChannels) {
                queryBuilder.addFilter(
                        TermParameter.of(
                            CONTENT_MAPPING.getBroadcasts().getBroadcastOn(),
                            scheduleChannel));
            }

            List<String> onDemandAvailables = distinctSplit(
                    queryBuilder,
                    CONTENT_MAPPING.getLocations().getAvailable(),
                    onDemandAvailableParam
            );
            for (String onDemandAvailable : onDemandAvailables) {
                boolean onDemandAvailableBoolean = Boolean.parseBoolean(onDemandAvailable);
                queryBuilder.addFilter(
                        TermParameter.of(
                            CONTENT_MAPPING.getLocations().getAvailable(),
                                onDemandAvailableBoolean));
            }

            SearchQuery searchQuery = queryBuilder
                    .withLimit(limit)
                    .withOffset(offset)
                    .build();

            List<Identified> content = searcher.search(searchQuery);

            resultWriter.write(QueryResult.listResult(
                    Iterables.filter(content, Content.class),
                    createQueryContext(request),
                    Long.valueOf(content.size())
            ), writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private List<String> distinctSplit(
            SearchQuery.Builder searchQuery,
            ChildTypeMapping<?> mapping,
            String parameter
    ) {
        if (parameter == null) {
            return ImmutableList.of();
        } else {
            List<String> parameters = Arrays.stream(parameter.split(String.valueOf(VALUE_SEPARATOR), -1))
                    .distinct()
                    .collect(Collectors.toList());
            handleExists(searchQuery, mapping, parameters);
            return parameters;
        }
    }

    private void handleExists(
            SearchQuery.Builder searchQuery,
            ChildTypeMapping<?> mapping,
            List<String> parameters
    ) {
        if (parameters.contains(EXISTS_KEYWORD)) {
            searchQuery.addFilter(ExistParameter.exists(mapping));
            parameters.remove(EXISTS_KEYWORD);
        } else if (parameters.contains(NON_EXISTS_KEYWORD)) {
            searchQuery.addFilter(ExistParameter.notExists(mapping));
            parameters.remove(NON_EXISTS_KEYWORD);
        }
    }

    private QueryContext createQueryContext(HttpServletRequest request)
            throws ApplicationResolutionException, InvalidAnnotationException,
            MissingAnnotationException, InvalidParameterException {
        return QueryContext.create(
                applicationFetcher.applicationFor(request).orElseThrow(InvalidParameterException::new),
                annotationsExtractor.extractFromSingleRequest(request),
                selectionBuilder.build(request),
                request
        );
    }
}
