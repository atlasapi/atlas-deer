package org.atlasapi.application.auth;

import javax.servlet.http.HttpServletRequest;
import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.ApplicationStore;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

public class ApiKeySourcesFetcher implements ApplicationSourcesFetcher {

    public static final String API_KEY_QUERY_PARAMETER = "apiKey";
    
    private final ApplicationStore reader;

    public ApiKeySourcesFetcher(ApplicationStore reader) {
        this.reader = reader;
    }
    
    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(API_KEY_QUERY_PARAMETER);
    }

    @Override
    public Optional<ApplicationSources> sourcesFor(HttpServletRequest request) throws RevokedApiKeyException, ApiKeyNotFoundException  {
            String apiKey = request.getParameter(API_KEY_QUERY_PARAMETER);
            if (apiKey != null) {
                Optional<Application> app = reader.applicationForKey(apiKey);
                if (app.isPresent()) {
                    if (app.get().isRevoked()) {
                        throw new RevokedApiKeyException(app.get().getCredentials().getApiKey());
                    }
                    return Optional.of(app.get().getSources());
                } else {
                    throw new ApiKeyNotFoundException(apiKey);
                }
            }
        return Optional.absent();
    }
}
