package org.atlasapi.application.auth.www;

import static org.atlasapi.application.auth.OAuthTokenUserFetcher.OAUTH_PROVIDER_QUERY_PARAMETER;
import static org.atlasapi.application.auth.OAuthTokenUserFetcher.OAUTH_TOKEN_QUERY_PARAMETER;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.common.properties.Configurer;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.application.users.User;
import org.atlasapi.output.ErrorResultWriter;
import org.atlasapi.output.ErrorSummary;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotAuthenticatedException;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.ResponseWriterFactory;
import org.atlasapi.output.UnsupportedFormatException;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.url.Urls;

@Controller
public class AuthController {
    private final ResponseWriterFactory writerResolver = new ResponseWriterFactory();
    private static Logger log = LoggerFactory.getLogger(AuthController.class);
    private final QueryResultWriter<OAuthProvider> resultWriter;
    private final UserFetcher userFetcher;
    private final NumberToShortStringCodec idCodec;
    private final String USER_URL = Configurer.get("atlas.uri").toString() + "/4/users/%s.%s";
    
    public AuthController(QueryResultWriter<OAuthProvider> resultWriter,
            UserFetcher userFetcher,
            NumberToShortStringCodec idCodec) {
        this.resultWriter = resultWriter;
        this.userFetcher = userFetcher;
        this.idCodec = idCodec;
    }
    
    @RequestMapping(value = { "/4/auth/providers.*" }, method = RequestMethod.GET) 
    public void listAuthProviders(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            QueryResult<OAuthProvider> queryResult = QueryResult.listResult(OAuthProvider.all(), QueryContext.standard(request));
            resultWriter.write(queryResult, writer);
        }  catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            ErrorSummary summary = ErrorSummary.forException(e);
            new ErrorResultWriter().write(summary, writer, request, response);
        }
    }
    
    @RequestMapping(value = { "/4/auth/user.json" }, method = RequestMethod.GET) 
    public void redirectToCurrentUser(HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam (value = OAUTH_PROVIDER_QUERY_PARAMETER) String oauthProvider,
            @RequestParam (value = OAUTH_TOKEN_QUERY_PARAMETER) String oauthToken) throws IOException {
        ResponseWriter writer = null;
        try {
            writer = writerResolver.writerFor(request, response);
            Optional<User> user = userFetcher.userFor(request);
            // We should always have a user at this point. However, there are
            // cases where the OAuthInterceptor is passing, yet there is no
            // user. Therefore we will check for the presence of a user here.
            if (!user.isPresent()) {
                writeError(new NotAuthenticatedException(), writer, request, response);
                return;
            }
            String userUrl = String.format(USER_URL, 
                    idCodec.encode(BigInteger.valueOf(user.get().getId().longValue())),
                    "json");
            Map<String, String> oauthParams = Maps.newHashMap();
            oauthParams.put(OAUTH_PROVIDER_QUERY_PARAMETER, oauthProvider);
            oauthParams.put(OAUTH_TOKEN_QUERY_PARAMETER, oauthToken);
            response.setStatus(HttpServletResponse.SC_FOUND);
            response.sendRedirect(Urls.appendParameters(userUrl, oauthParams));
        }  catch (Exception e) {
            log.error("Request exception " + request.getRequestURI(), e);
            writeError(e, writer, request, response);
            return;
        }
    }
    
    private void writeError(Exception exception, ResponseWriter writer, 
            HttpServletRequest request, HttpServletResponse response) throws IOException {
        ErrorSummary summary = ErrorSummary.forException(exception);
        new ErrorResultWriter().write(summary, writer, request, response);
    }
}
