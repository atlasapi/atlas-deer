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
import org.atlasapi.query.common.exceptions.InvalidParameterException;
import org.atlasapi.query.v2.ParameterChecker;
import org.atlasapi.query.v4.topic.TopicController;
import org.atlasapi.query.v5.search.attribute.SherlockAttribute;
import org.atlasapi.query.v5.search.attribute.SherlockAttributes;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.search.parameter.FilterParameter;
import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;

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

    private static final String ANNOTATIONS_PARAM = "annotations";
    private static final String QUERY_PARAM = "q";

    private final ContentResolvingSearcher searcher;
    private final ApplicationFetcher applicationFetcher;
    private final AnnotationsExtractor annotationsExtractor;
    private final Selection.SelectionBuilder selectionBuilder;
    private final QueryResultWriter<Content> resultWriter;
    private final List<SherlockAttribute<?, ?>> sherlockAttributes;
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private final ParameterChecker paramChecker = new ParameterChecker(ImmutableSet.<String>builder()
                .add(ApiKeyApplicationFetcher.API_KEY_QUERY_PARAMETER)
                .add(Selection.LIMIT_REQUEST_PARAM)
                .add(Selection.START_INDEX_REQUEST_PARAM)
                .add(JsonResponseWriter.CALLBACK)
                .add(ANNOTATIONS_PARAM)
                .add(QUERY_PARAM)
                .addAll(SherlockAttributes.all())
                .build()
    );

    public SearchController(
            ContentResolvingSearcher searcher,
            ApplicationFetcher applicationFetcher,
            List<SherlockAttribute<?, ?>> sherlockAttributes,
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
        for (SherlockAttribute<?, ?> attribute : sherlockAttributes) {

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
