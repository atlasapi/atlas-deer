package org.atlasapi.application.auth;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.ApplicationStore;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class ApiKeySourcesFetcher implements ApplicationSourcesFetcher {

    public static final String API_KEY_QUERY_PARAMETER = "key";
    
    private final ApplicationStore reader;

    public ApiKeySourcesFetcher(ApplicationStore reader) {
        this.reader = checkNotNull(reader);
    }
    
    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of();
    }

    @Override
    public Optional<ApplicationSources> sourcesFor(HttpServletRequest request) throws InvalidApiKeyException  {
        String apiKey = request.getParameter(API_KEY_QUERY_PARAMETER);
        if (apiKey == null) {
            apiKey = request.getHeader(API_KEY_QUERY_PARAMETER);
        }
        if (apiKey != null) {
            Optional<Application> app = reader.applicationForKey(apiKey);
            if (!app.isPresent() || app.get().isRevoked()) {
                throw new InvalidApiKeyException(apiKey);
            }
            return Optional.of(app.get().getSources());
        }
        return Optional.absent();
    }
}
