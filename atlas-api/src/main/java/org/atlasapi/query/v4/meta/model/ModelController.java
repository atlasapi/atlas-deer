package org.atlasapi.query.v4.meta.model;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.InvalidApiKeyException;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.generation.ModelClassInfoStore;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.query.common.ContextualQueryContextParser;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ModelController {

    private static final Logger log = LoggerFactory.getLogger(ModelController.class);

    private final ModelClassInfoStore classInfoStore;
    private final QueryResultWriter<ModelClassInfo> resultWriter;
    private final ContextualQueryContextParser contextParser;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();

    public ModelController(ModelClassInfoStore classInfoStore,
            QueryResultWriter<ModelClassInfo> resultWriter,
            ContextualQueryContextParser contextParser) {
        this.classInfoStore = checkNotNull(classInfoStore);
        this.resultWriter = checkNotNull(resultWriter);
        this.contextParser = checkNotNull(contextParser);
    }

    // TODO if model classes can have an Id, then this can be modified closer to the rest of the endpoints,
    // as the rest of the query parsing and query execution framework can be used. it might make annotations
    // a little more friendly
    // also means that the resolution/result creation methods can be removed from this controller class
    @RequestMapping({ "/4/meta/types\\.[a-z]+", "/4/meta/types" })
    public void fetchAllModelInfo(HttpServletRequest request, HttpServletResponse response)
            throws IOException, UnsupportedFormatException, NotAcceptableException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);

            Iterable<ModelClassInfo> resources = resolveAllResources();
            QueryResult<ModelClassInfo> queryResult = createListResultFrom(resources, request);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private Iterable<ModelClassInfo> resolveAllResources() {
        return classInfoStore.allClassInformation();
    }

    private QueryResult<ModelClassInfo> createListResultFrom(Iterable<ModelClassInfo> resources,
            HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return QueryResult.listResult(
                resources,
                contextParser.parseContext(request),
                ImmutableList.copyOf(resources).size()
        );
    }

    @RequestMapping({ "/4/meta/types/{key}\\.[a-z]+", "/4/meta/types/{key}" })
    public void fetchSingleModelInfo(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("key") String key) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);

            ModelClassInfo resolved = resolveResource(request, key);
            QueryResult<ModelClassInfo> queryResult = createSingleResultFrom(resolved, request);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private ModelClassInfo resolveResource(HttpServletRequest request, String key)
            throws QueryExecutionException {
        Optional<ModelClassInfo> classInfo = classInfoStore.classInfoFor(key);
        if (!classInfo.isPresent()) {
            throw new QueryExecutionException("no resource found with key " + key);
        }
        return classInfo.get();
    }

    private QueryResult<ModelClassInfo> createSingleResultFrom(ModelClassInfo resource,
            HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return QueryResult.singleResult(resource, contextParser.parseContext(request));
    }
}
