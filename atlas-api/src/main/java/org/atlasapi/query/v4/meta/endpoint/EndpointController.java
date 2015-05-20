package org.atlasapi.query.v4.meta.endpoint;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.generation.EndpointClassInfoStore;
import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.ContextualQueryContextParser;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryParseException;
import org.atlasapi.query.common.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.google.common.base.Optional;

@Controller
public class EndpointController {

    private static final Logger log = LoggerFactory.getLogger(EndpointController.class);
    
    private final EndpointClassInfoStore endpointInfoStore;
    private final QueryResultWriter<EndpointClassInfo> resultWriter;
    private final ContextualQueryContextParser contextParser;

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    
    public EndpointController(EndpointClassInfoStore endpointInfoStore, QueryResultWriter<EndpointClassInfo> resultWriter, 
            ContextualQueryContextParser contextParser) {
        this.endpointInfoStore = checkNotNull(endpointInfoStore);
        this.resultWriter = checkNotNull(resultWriter);
        this.contextParser = checkNotNull(contextParser);
    }
    
    @RequestMapping({"/4/meta/resources.*", "/4/meta/resources"})
    public void fetchAllEndpointInfo(HttpServletRequest request, HttpServletResponse response) throws IOException { 
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);

            Iterable<EndpointClassInfo> resources = resolveAllResources();
            QueryResult<EndpointClassInfo> queryResult = createListResultFrom(resources, request);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    private Iterable<EndpointClassInfo> resolveAllResources() {
        return endpointInfoStore.allEndpointInformation();
    }

    private QueryResult<EndpointClassInfo> createListResultFrom(Iterable<EndpointClassInfo> resources, HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return QueryResult.listResult(resources, contextParser.parseContext(request));
    }
    
    @RequestMapping({"/4/meta/resources/{key}.*", "/4/meta/resources/{key}"})
    public void fetchSingleEndpointInfo(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("key") String key) throws IOException { 
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);

            EndpointClassInfo resolved = resolveResource(request, key);
            QueryResult<EndpointClassInfo> queryResult = createSingleResultFrom(resolved, request);
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }

    private EndpointClassInfo resolveResource(HttpServletRequest request, String key) throws QueryExecutionException {
        Optional<EndpointClassInfo> classInfo = endpointInfoStore.endpointInfoFor(key);
        if (!classInfo.isPresent()) {
            throw new QueryExecutionException("no resource found with key " + key);
        }
        return classInfo.get();
    }

    private QueryResult<EndpointClassInfo> createSingleResultFrom(EndpointClassInfo resource, HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        return QueryResult.singleResult(resource, contextParser.parseContext(request));
    }
}
