package org.atlasapi.query.v4.search;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.application.DefaultApplication;
import org.atlasapi.application.ApiKeyApplicationFetcher;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.v2.ParameterChecker;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResolver;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.text.MoreStrings;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class SearchController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private static final String QUERY_PARAM = "q";
    private static final String SPECIALIZATION_PARAM = "specialization";
    private static final String PUBLISHER_PARAM = "publisher";
    private static final String TITLE_WEIGHTING_PARAM = "titleWeighting";
    private static final String BROADCAST_WEIGHTING_PARAM = "broadcastWeighting";
    private static final String CATCHUP_WEIGHTING_PARAM = "catchupWeighting";
    private static final String TYPE_PARAM = "type";
    private static final String TOP_LEVEL_PARAM = "topLevelOnly";
    private static final String CURRENT_BROADCASTS_ONLY = "currentBroadcastsOnly";
    private static final String PRIORITY_CHANNEL_WEIGHTING = "priorityChannelWeighting";
    private static final String ANNOTATIONS_PARAM = "annotations";

    private static final float DEFAULT_TITLE_WEIGHTING = 1.0f;
    private static final float DEFAULT_PRIORITY_CHANNEL_WEIGHTING = 1.0f;
    private static final float DEFAULT_BROADCAST_WEIGHTING = 0.2f;
    private static final float DEFAULT_CATCHUP_WEIGHTING = 0.15f;

    private final SearchResolver searcher;
    private final ApplicationFetcher applicationFetcher;
    private final QueryResultWriter<ResolvedContent> resultWriter;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    private final ParameterChecker paramChecker = new ParameterChecker(ImmutableSet.of(
            ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER,
            Selection.LIMIT_REQUEST_PARAM,
            Selection.START_INDEX_REQUEST_PARAM,
            QUERY_PARAM,
            SPECIALIZATION_PARAM,
            PUBLISHER_PARAM,
            TITLE_WEIGHTING_PARAM,
            BROADCAST_WEIGHTING_PARAM,
            CATCHUP_WEIGHTING_PARAM,
            JsonResponseWriter.CALLBACK,
            ANNOTATIONS_PARAM,
            TYPE_PARAM,
            TOP_LEVEL_PARAM,
            CURRENT_BROADCASTS_ONLY,
            PRIORITY_CHANNEL_WEIGHTING
    ));

    public SearchController(SearchResolver searcher, ApplicationFetcher applicationFetcher,
            QueryResultWriter<ResolvedContent> resultWriter) {
        this.searcher = searcher;
        this.applicationFetcher = applicationFetcher;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "/4/search\\.[a-z]+", "/4/search" })
    public void search(@RequestParam(QUERY_PARAM) String q,
            @RequestParam(value = SPECIALIZATION_PARAM, required = false) String specialization,
            @RequestParam(value = PUBLISHER_PARAM, required = false) String publisher,
            @RequestParam(value = TITLE_WEIGHTING_PARAM,
                    required = false) String titleWeightingParam,
            @RequestParam(value = BROADCAST_WEIGHTING_PARAM,
                    required = false) String broadcastWeightingParam,
            @RequestParam(value = CATCHUP_WEIGHTING_PARAM,
                    required = false) String catchupWeightingParam,
            @RequestParam(value = TYPE_PARAM, required = false) String type,
            @RequestParam(value = TOP_LEVEL_PARAM, required = false,
                    defaultValue = "true") String topLevel,
            @RequestParam(value = CURRENT_BROADCASTS_ONLY, required = false,
                    defaultValue = "false") String currentBroadcastsOnly,
            @RequestParam(value = PRIORITY_CHANNEL_WEIGHTING,
                    required = false) String priorityChannelWeightingParam,
            HttpServletRequest request, HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            paramChecker.checkParameters(request);

            if (Strings.isNullOrEmpty(q)) {
                throw new IllegalArgumentException("You must specify a query parameter");
            }

            Selection selection = Selection.builder().build(request);
            if (!selection.hasLimit()) {
                throw new IllegalArgumentException("You must specify a limit parameter");
            }

            float titleWeighting = getFloatParam(titleWeightingParam, DEFAULT_TITLE_WEIGHTING);
            float broadcastWeighting = getFloatParam(
                    broadcastWeightingParam,
                    DEFAULT_BROADCAST_WEIGHTING
            );
            float catchupWeighting = getFloatParam(
                    catchupWeightingParam,
                    DEFAULT_CATCHUP_WEIGHTING
            );
            float priorityChannelWeighting = getFloatParam(
                    priorityChannelWeightingParam,
                    DEFAULT_PRIORITY_CHANNEL_WEIGHTING
            );

            Application application = applicationFetcher.applicationFor(request)
                    .orElse(DefaultApplication.create());
            Set<Specialization> specializations = specializations(specialization);
            Set<Publisher> publishers = publishers(publisher, application);
            List<Identified> content = searcher.search(SearchQuery.builder(q)
                    .withSelection(selection)
                    .withSpecializations(specializations)
                    .withPublishers(publishers)
                    .withTitleWeighting(titleWeighting)
                    .withBroadcastWeighting(broadcastWeighting)
                    .withCatchupWeighting(catchupWeighting)
                    .withPriorityChannelWeighting(priorityChannelWeighting)
                    .withType(type)
                    .isTopLevelOnly(!Strings.isNullOrEmpty(topLevel)
                                    ? Boolean.valueOf(topLevel)
                                    : null)
                    .withCurrentBroadcastsOnly(!Strings.isNullOrEmpty(currentBroadcastsOnly)
                                               ? Boolean.valueOf(currentBroadcastsOnly)
                                               : false)
                    .build(), application);

            resultWriter.write(
                    QueryResult.listResult(
                            makeResolved(content),
                            QueryContext.standard(request),
                            Long.valueOf(content.size())
                    ),
                    writer
            );

        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private Iterable<ResolvedContent> makeResolved(Iterable<Identified> contents) {
        return StreamSupport.stream(contents.spliterator(), false)
                .filter(content -> content instanceof Content)
                .map(content -> (Content) content)
                .map(content -> ResolvedContent.builder().withContent(content).build())
                .collect(MoreCollectors.toImmutableList());

    }

    private Set<Publisher> publishers(String publisher, Application application) {
        return Sets.intersection(
                ImmutableSet.copyOf(Publisher.fromCsv(publisher)),
                application.getConfiguration().getEnabledReadSources()
        );
    }

    private float getFloatParam(String stringValue, float defaultValue) {
        if (!Strings.isNullOrEmpty(stringValue)) {
            if (MoreStrings.containsOnlyDecimalCharacters(stringValue)) {
                return Float.parseFloat(stringValue);
            }
        }
        return defaultValue;
    }

    protected Set<Specialization> specializations(String specializationString) {
        if (specializationString != null) {
            ImmutableSet.Builder<Specialization> specializations = ImmutableSet.builder();
            for (String s : Splitter.on(",")
                    .omitEmptyStrings()
                    .trimResults()
                    .split(specializationString)) {
                Maybe<Specialization> specialization = Specialization.fromKey(s);
                if (specialization.hasValue()) {
                    specializations.add(specialization.requireValue());
                }
            }
            return specializations.build();
        } else {
            return Sets.newHashSet();
        }
    }
}
