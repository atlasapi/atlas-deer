package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.ApiKeyApplicationFetcher;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.content.Content;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.annotation.AnnotationsExtractor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.coercers.BooleanCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;
import org.atlasapi.query.common.exceptions.InvalidParameterException;
import org.atlasapi.query.v2.ParameterChecker;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.query.v5.search.attribute.BooleanDateAttribute;
import org.atlasapi.query.v5.search.attribute.InstantRangeCoercer;
import org.atlasapi.query.v5.search.attribute.IntegerRangeCoercer;
import org.atlasapi.query.v5.search.attribute.RangeAttribute;
import org.atlasapi.query.v5.search.attribute.SherlockAttribute;
import org.atlasapi.query.v5.search.attribute.TermAttribute;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.search.parameter.FilterParameter;
import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class SearchController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    private final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
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

    private final List<SherlockAttribute<?, ?>> attributes =
            ImmutableList.<SherlockAttribute<?, ?>>builder()
                    .add(new RangeAttribute<>(
                            YEAR_PARAM,
                            CONTENT_MAPPING.getYear(),
                            IntegerRangeCoercer.create()
                    ))
                    .add(new TermAttribute<>(
                            TYPE_PARAM,
                            CONTENT_MAPPING.getType(),
                            StringCoercer.create()
                    ))
                    .add(new TermAttribute<>(
                            PUBLISHER_PARAM,
                            CONTENT_MAPPING.getSource().getKey(),
                            StringCoercer.create()
                    ))
                    .add(new BooleanDateAttribute(
                            SCHEDULE_UPCOMING_PARAM,
                            CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime()
                    ))
                    .add(new RangeAttribute<>(
                            SCHEDULE_TIME_PARAM,
                            CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                            InstantRangeCoercer.create()
                    ))
                    .add(new TermAttribute<>(
                            SCHEDULE_CHANNEL_PARAM,
                            CONTENT_MAPPING.getBroadcasts().getBroadcastOn(),
                            StringCoercer.create()
                    ))
                    .add(new TermAttribute<>(
                            ON_DEMAND_AVAILABLE_PARAM,
                            CONTENT_MAPPING.getLocations().getAvailable(),
                            BooleanCoercer.create()
                    ))
                    .build();

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
            @RequestParam(value = QUERY_PARAM, required = false) String query,
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            paramChecker.checkParameters(request);

            List<NamedParameter<?>> parameters = parse(request);

            SearchQuery.Builder queryBuilder;
            if (Strings.isNullOrEmpty(query)) {
                queryBuilder = SearchQuery.builder();
            } else {
                queryBuilder = SearchQuery.getDefaultQuerySearcher(query);
            }

            for (NamedParameter<?> parameter : parameters) {
                if (parameter instanceof SearchParameter) {
                    queryBuilder.addSearcher((SearchParameter)parameter);
                } else {
                    queryBuilder.addFilter((FilterParameter<?>)parameter);
                }
            }

            Selection selection = selectionBuilder.build(request);

            SearchQuery searchQuery = queryBuilder
                    .withLimit(selection.getLimit())
                    .withOffset(selection.getOffset())
                    .build();

            QueryContext queryContext = QueryContext.create(
                    applicationFetcher.applicationFor(request)
                            .orElseThrow(InvalidParameterException::new),
                    annotationsExtractor.extractFromSingleRequest(request),
                    request
            );

            QueryResult<Content> contentResult = searcher.search(searchQuery, queryContext);
            resultWriter.write(contentResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private List<NamedParameter<?>> parse(HttpServletRequest request)
            throws InvalidAttributeValueException {

        List<NamedParameter<?>> sherlockParameters = new ArrayList<>();

        Map<String, String[]> parameterMap = (Map<String, String[]>) request.getParameterMap();
        for (SherlockAttribute<?, ?> attribute : attributes) {

            String name = attribute.getParameterName();
            if (parameterMap.containsKey(name)) {

                List<String> values = Arrays.stream(parameterMap.get(name))
                        .map(SPLITTER::splitToList)
                        .flatMap(Collection::stream)
                        .distinct()
                        .collect(Collectors.toList());

                if (values.contains(EXISTS_KEYWORD)) {
                    sherlockParameters.add(ExistParameter.exists(attribute.getMapping()));
                    values.remove(EXISTS_KEYWORD);
                } else if (values.contains(NON_EXISTS_KEYWORD)) {
                    sherlockParameters.add(ExistParameter.notExists(attribute.getMapping()));
                    values.remove(NON_EXISTS_KEYWORD);
                }

                sherlockParameters.addAll(attribute.coerce(values));
            }
        }

        return sherlockParameters;
    }
}
