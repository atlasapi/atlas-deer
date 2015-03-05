package org.atlasapi.users.videosource;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.users.videosource.model.VideoSourceOAuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class VideoSourceController {

    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private static Logger log = LoggerFactory.getLogger(VideoSourceController.class);
    private final QueryResultWriter<VideoSourceOAuthProvider> resultWriter;
    private final UserFetcher userFetcher;

    public VideoSourceController(QueryResultWriter<VideoSourceOAuthProvider> resultWriter,
            UserFetcher userFetcher) {
        this.resultWriter = resultWriter;
        this.userFetcher = userFetcher;
    }

    @RequestMapping(value = { "/4/videosource/providers.*" }, method = RequestMethod.GET)
    public void listAuthProviders(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            QueryResult<VideoSourceOAuthProvider> queryResult =
                    QueryResult.listResult(VideoSourceOAuthProvider.all(), QueryContext.standard(request));
            resultWriter.write(queryResult, writer);
        } catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
}
