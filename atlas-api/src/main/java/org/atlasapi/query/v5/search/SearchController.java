package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApiKeyApplicationFetcher;
import org.atlasapi.application.ApplicationFetcher;
import org.atlasapi.application.DefaultApplication;
import org.atlasapi.content.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.ProducesType;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.annotation.AnnotationsExtractor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;
import org.atlasapi.query.v2.ParameterChecker;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.query.v5.search.attribute.SherlockAttribute;
import org.atlasapi.query.v5.search.attribute.SherlockSingleMappingAttribute;
import org.atlasapi.query.v5.search.attribute.SherlockParameter;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.parameter.Parameter;
import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.client.search.scoring.QueryWeighting;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@ProducesType(type = Content.class)
@Controller
@RequestMapping("/5/search")
public class SearchController {

    private static Logger log = LoggerFactory.getLogger(TopicController.class);

    private final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
    private static final String EXISTS_KEYWORD = "nonNull";
    private static final String NON_EXISTS_KEYWORD = "null";
    private static final ContentMapping CONTENT = IndexMapping.getContent();

    private static final String ANNOTATIONS_PARAM = "annotations";
    private static final String QUERY_PARAM = "q";
    private static final String API_KEY_PARAM = ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER;

    private final ContentResolvingSearcher searcher;
    private final ApplicationFetcher applicationFetcher;
    private final AnnotationsExtractor annotationsExtractor;
    private final Selection.SelectionBuilder selectionBuilder;
    private final QueryResultWriter<Content> resultWriter;
    private final List<SherlockAttribute<?, ?, ?, ?>> sherlockAttributes;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final ParameterChecker paramChecker = new ParameterChecker(ImmutableSet.<String>builder()
                .add(API_KEY_PARAM)
                .add(Selection.LIMIT_REQUEST_PARAM)
                .add(Selection.START_INDEX_REQUEST_PARAM)
                .add(JsonResponseWriter.CALLBACK)
                .add(ANNOTATIONS_PARAM)
                .add(QUERY_PARAM)
                .addAll(SherlockParameter.getAllNames())
                .build()
    );

    public SearchController(
            ContentResolvingSearcher searcher,
            ApplicationFetcher applicationFetcher,
            List<SherlockAttribute<?, ?, ?, ?>> sherlockAttributes,
            AnnotationsExtractor annotationsExtractor,
            Selection.SelectionBuilder selectionBuilder,
            QueryResultWriter<Content> resultWriter
    ) {
        this.searcher = searcher;
        this.applicationFetcher = applicationFetcher;
        this.sherlockAttributes = sherlockAttributes;
        this.annotationsExtractor = annotationsExtractor;
        this.selectionBuilder = selectionBuilder;
        this.resultWriter = resultWriter;
    }

    @RequestMapping({ "\\.[a-z]+", "" })
    public void search(
            @RequestParam(value = QUERY_PARAM, required = false) String query,
            @RequestParam(value = API_KEY_PARAM) String apiKey,
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            paramChecker.checkParameters(request);

            QueryContext queryContext = QueryContext.create(
                    applicationFetcher.applicationFor(request).orElse(DefaultApplication.create()),
                    annotationsExtractor.extractFromSingleRequest(request),
                    request
            );

            SearchQuery.Builder queryBuilder;
            if (Strings.isNullOrEmpty(query)) {
                queryBuilder = SearchQuery.builder();
            } else {
                if (queryContext.getAnnotations().values().contains(Annotation.NO_SMART_SEARCH)) {
                    queryBuilder = SearchQuery.getDefaultQuerySearcher(query, false);
                } else {
                    queryBuilder = SearchQuery.getDefaultQuerySearcher(query, true);
                }
            }

            List<? extends Parameter> parameters = parseSherlockParameters(request, queryContext);
            for (Parameter parameter : parameters) {
                if (parameter instanceof SearchParameter) {
                    queryBuilder.addSearcher((SearchParameter)parameter);
                } else {
                    queryBuilder.addFilter(parameter);
                }
            }

            Selection selection = selectionBuilder.build(request);

            SearchQuery searchQuery = queryBuilder
                    .withQueryWeighting(parseQueryWeighting(request))
                    .withLimit(selection.getLimit())
                    .withOffset(selection.getOffset())
                    .build();

            QueryResult<Content> contentResult = searcher.search(searchQuery, queryContext);
            resultWriter.write(contentResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private List<? extends Parameter> parseSherlockParameters(
            HttpServletRequest request,
            QueryContext queryContext
    ) throws InvalidAttributeValueException {

        Set<String> enabledReadSourcesKeys = queryContext.getApplication()
                .getConfiguration()
                .getEnabledReadSources()
                .stream()
                .map(Publisher::key)
                .collect(Collectors.toSet());

        List<Parameter> sherlockParameters = new ArrayList<>();
        Map<String, String[]> parameterMap = (Map<String, String[]>) request.getParameterMap();
        for (SherlockAttribute<?, ?, ?, ?> attribute : sherlockAttributes) {

            SherlockParameter parameter = attribute.getParameter();
            if (parameterMap.containsKey(parameter.getParameterName())) {

                List<String> values = Arrays.stream(parameterMap.get(parameter.getParameterName()))
                        .map(SPLITTER::splitToList)
                        .flatMap(Collection::stream)
                        .distinct()
                        .collect(Collectors.toList());

                if (attribute instanceof SherlockSingleMappingAttribute) {
                    SherlockSingleMappingAttribute<?, ?, ?> singleMappingAttribute =
                            (SherlockSingleMappingAttribute<?, ?, ?>) attribute;
                    if (values.contains(EXISTS_KEYWORD)) {
                        sherlockParameters.add(singleMappingAttribute.getExistsParameter(true));
                        values.remove(EXISTS_KEYWORD);
                    } else if (values.contains(NON_EXISTS_KEYWORD)) {
                        sherlockParameters.add(singleMappingAttribute.getExistsParameter(false));
                        values.remove(NON_EXISTS_KEYWORD);
                    }
                }

                try {
                    List<? extends Parameter> coercedValues = attribute.coerce(values);

                    if (parameter == SherlockParameter.PUBLISHER) {
                        List<SimpleParameter<String>> parsedPublishers =
                                (List<SimpleParameter<String>>) coercedValues;
                        for (SimpleParameter<String> parsedPublisher : parsedPublishers) {
                            if (!enabledReadSourcesKeys.contains(parsedPublisher.getValue())) {
                                throw new InvalidAttributeValueException(String.format(
                                        "The source %s is not within the scope of the provided api key",
                                        parsedPublisher.getValue()));
                            }
                        }
                    }

                    sherlockParameters.addAll(coercedValues);

                } catch (InvalidAttributeValueException e) {
                    throw new InvalidAttributeValueException(String.format(
                            "Invalid value(s) for %s: %s.",
                            parameter.getParameterName(),
                            e.getMessage()
                    ), e);
                }

            // if no values provided for publisher, add all keys from the application key set
            } else if (parameter == SherlockParameter.PUBLISHER) {
                for (String enabledReadSourceKey : enabledReadSourcesKeys) {
                    sherlockParameters.add(TermParameter.of(
                            CONTENT.getSource().getKey(),
                            enabledReadSourceKey));
                }
            }
        }

        return sherlockParameters;
    }

    private QueryWeighting parseQueryWeighting(HttpServletRequest request) {
        return QueryWeighting.defaultQueryWeighting();
    }
}