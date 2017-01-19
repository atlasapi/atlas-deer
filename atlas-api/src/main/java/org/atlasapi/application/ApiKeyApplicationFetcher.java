package org.atlasapi.application;

import javax.servlet.http.HttpServletRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.Environment;
import com.metabroadcast.applications.client.query.Query;
import com.metabroadcast.applications.client.query.Result;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.properties.Configurer;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ApiKeyApplicationFetcher implements ApplicationFetcher {

    public static final String API_KEY_QUERY_PARAMETER = "key";

    private final ApplicationsClient applicationsClient;
    private final Environment environment;

    @VisibleForTesting
    ApiKeyApplicationFetcher(ApplicationsClient applicationsClient, Environment environment) {
        this.applicationsClient = checkNotNull(applicationsClient);
        this.environment = checkNotNull(environment);
    }

    public static ApiKeyApplicationFetcher create(ApplicationsClient applicationsClient) {
        return new ApiKeyApplicationFetcher(
                applicationsClient,
                Environment.parse(Configurer.getPlatform())
        );
    }

    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(API_KEY_QUERY_PARAMETER);
    }

    @Override
    public Optional<Application> applicationFor(HttpServletRequest request)
            throws InvalidApiKeyException {

        String apiKey;
        try {

            apiKey = MoreObjects.firstNonNull(
                    request.getParameter(API_KEY_QUERY_PARAMETER),
                    request.getHeader(API_KEY_QUERY_PARAMETER)
            );
        } catch (NullPointerException e) {
            throw InvalidApiKeyException.create(e.getMessage(), "No api key supplied");
        }

        Result result = applicationsClient.resolve(
                Query.create(apiKey, environment)
        );

        if (result.getErrorCode().isPresent()) {
            throw InvalidApiKeyException.create(apiKey, result.getErrorCode().get().toString());
        }
        return result.getSingleResult();
    }
}
