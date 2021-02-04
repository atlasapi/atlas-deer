package org.atlasapi.query.v4.search;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.sherlock.client.parameter.Parameter;
import com.metabroadcast.sherlock.client.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.client.scoring.QueryWeighting;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
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
import org.atlasapi.query.v4.search.attribute.SherlockAttribute;
import org.atlasapi.query.v4.search.attribute.SherlockParameter;
import org.atlasapi.query.v4.search.attribute.SherlockSingleMappingAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ProducesType(type = Content.class)
@Controller
@RequestMapping("/4/search/content")
public class SearchController {

    private static final Logger log = LoggerFactory.getLogger(SearchController.class);

    private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private static final String EXISTS_KEYWORD = "nonNull";
    private static final String NON_EXISTS_KEYWORD = "null";
    private static final ContentMapping CONTENT = IndexMapping.getContentMapping();

    private static final String ANNOTATIONS_PARAM = "annotations";
    private static final String QUERY_PARAM = "q";
    private static final String SMART_SEARCH_PARAM = "smart_search";

    private static final ParameterChecker PARAM_CHECKER = new ParameterChecker(
            ImmutableSet.<String>builder()
                    .add(ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER)
                    .add(Selection.LIMIT_REQUEST_PARAM)
                    .add(Selection.START_INDEX_REQUEST_PARAM)
                    .add(JsonResponseWriter.CALLBACK)
                    .add(ANNOTATIONS_PARAM)
                    .add(QUERY_PARAM)
                    .add(SMART_SEARCH_PARAM)
                    .addAll(SherlockParameter.getAllNames())
                    .build()
    );

    private final ContentResolvingSearcher searcher;
    private final ApplicationFetcher applicationFetcher;
    private final AnnotationsExtractor annotationsExtractor;
    private final Selection.SelectionBuilder selectionBuilder;
    private final QueryResultWriter<Content> resultWriter;
    private final List<SherlockAttribute<?, ?, ?, ?>> sherlockAttributes;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

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

    @RequestMapping({"\\.[a-z]+", ""})
    public void search(
            @RequestParam(value = QUERY_PARAM, required = false) String query,
            @RequestParam(value = SMART_SEARCH_PARAM, defaultValue = "true") boolean smartSearch,
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {

        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            PARAM_CHECKER.checkParameters(request);

            QueryContext queryContext = QueryContext.create(
                    applicationFetcher.applicationFor(request).orElse(DefaultApplication.create()),
                    annotationsExtractor.extractFromSingleRequest(request),
                    request
            );

            SearchQuery.Builder queryBuilder;
            if (Strings.isNullOrEmpty(query)) {
                queryBuilder = SearchQuery.builder();
            } else {
                queryBuilder = SearchQuery.getDefaultContentQuerySearcher(query, smartSearch);
            }

            parseSherlockParameters(request, queryContext, queryBuilder);

            Selection selection = selectionBuilder.build(request);

            SearchQuery.Builder searchQuery = queryBuilder
                    .withQueryWeighting(parseQueryWeighting(request));

            QueryResult<Content> contentResult = searcher.search(
                    searchQuery,
                    selection,
                    queryContext,
                    !Strings.isNullOrEmpty(query)
            );
            resultWriter.write(contentResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private void parseSherlockParameters(
            HttpServletRequest request,
            QueryContext queryContext,
            SearchQuery.Builder queryBuilder
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

                    if (parameter == SherlockParameter.SOURCE) {
                        List<SingleValueParameter<String>> parsedSources =
                                (List<SingleValueParameter<String>>) coercedValues;
                        for (SingleValueParameter<String> parsedSource : parsedSources) {
                            if (!enabledReadSourcesKeys.contains(parsedSource.getValue())) {
                                throw new InvalidAttributeValueException(String.format(
                                        "The source %s is not within the scope of the provided api key",
                                        parsedSource.getValue()));
                            }
                        }
                        Set<String> sources = parsedSources.stream()
                                .map(SingleValueParameter::getValue)
                                .collect(MoreCollectors.toImmutableSet());
                        queryBuilder.withIndex(SherlockIndex.CONTENT, sources);
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
            } else if (parameter == SherlockParameter.SOURCE) {
                for (String enabledReadSourceKey : enabledReadSourcesKeys) {
                    sherlockParameters.add(TermParameter.of(
                            CONTENT.getSource().getKey(),
                            enabledReadSourceKey));
                }
                queryBuilder.withIndex(SherlockIndex.CONTENT, enabledReadSourcesKeys);
            }
        }

        for (Parameter parameter : sherlockParameters) {
            if (parameter instanceof SearchParameter) {
                queryBuilder.addSearcher((SearchParameter) parameter);
            } else {
                queryBuilder.addFilter(parameter);
            }
        }
    }

    private QueryWeighting parseQueryWeighting(HttpServletRequest request) {
        return QueryWeighting.defaultContentQueryWeighting();
    }
}
