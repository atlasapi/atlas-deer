package org.atlasapi.application.auth;

import javax.servlet.http.HttpServletRequest;

import com.google.api.client.repackaged.com.google.common.base.Objects;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.Environment;
import com.metabroadcast.applications.client.query.Query;
import com.metabroadcast.applications.client.query.Result;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ApiKeyApplicationFetcher implements ApplicationFetcher {

    public static final String API_KEY_QUERY_PARAMETER = "key";

    private final ApplicationsClient applicationsClient;

    public ApiKeyApplicationFetcher(ApplicationsClient applicationsClient) {
        this.applicationsClient = checkNotNull(applicationsClient);
    }

    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(API_KEY_QUERY_PARAMETER);
    }

    @Override
    public Optional<Application> applicationFor(HttpServletRequest request)
            throws InvalidApiKeyException {

        String apiKey = Objects.firstNonNull(
                request.getParameter(API_KEY_QUERY_PARAMETER),
                request.getHeader(API_KEY_QUERY_PARAMETER)
        );

        if (apiKey != null) {
            Result result = applicationsClient.resolve(Query.create(apiKey, Environment.PROD)); //TODO: get environment from correct place
            if (result.getErrorCode().isPresent()) {
                throw new InvalidApiKeyException(result.getErrorCode().get().toString());
            }
            return result.getSingleResult();
        }
        return Optional.empty();
    }
}
