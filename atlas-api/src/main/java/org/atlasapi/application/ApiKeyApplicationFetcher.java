package org.atlasapi.application;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.Environment;
import com.metabroadcast.applications.client.query.Query;
import com.metabroadcast.applications.client.query.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ApiKeyApplicationFetcher implements ApplicationFetcher {

    public static final String API_KEY_QUERY_PARAMETER = "key";

    private static final Logger log = LoggerFactory.getLogger(ApiKeyApplicationFetcher.class);

    private final ApplicationsClient applicationsClient;
    private final Environment environment;

    @VisibleForTesting
    ApiKeyApplicationFetcher(ApplicationsClient applicationsClient, Environment environment) {
        this.applicationsClient = checkNotNull(applicationsClient);
        this.environment = checkNotNull(environment);
    }

    public static ApiKeyApplicationFetcher create(
            ApplicationsClient applicationsClient,
            String environment
    ) {
        return new ApiKeyApplicationFetcher(
                applicationsClient,
                Environment.parse(environment)
        );
    }

    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(API_KEY_QUERY_PARAMETER);
    }

    @Override
    public Optional<Application> applicationFor(HttpServletRequest request)
            throws ApplicationResolutionException {

        String apiKeyParam = request.getParameter(API_KEY_QUERY_PARAMETER);
        String apiKeyHeader = request.getHeader(API_KEY_QUERY_PARAMETER);

        if (Strings.isNullOrEmpty(apiKeyParam) && Strings.isNullOrEmpty(apiKeyHeader)) {
            return Optional.empty();
        }

        String apiKey = MoreObjects.firstNonNull(apiKeyParam, apiKeyHeader);

        return applicationForApiKey(apiKey);
    }

    @Override
    public Optional<Application> applicationForApiKey(String apiKey) throws ApplicationResolutionException {
        Result result = applicationsClient.resolve(
                Query.create(apiKey, environment)
        );

        if (result.getErrorCode().isPresent()) {
            log.error("Unable to resolve application: {} - {}", result.getErrorCode().get(), apiKey);
            throw ApplicationResolutionException.create(
                    apiKey,
                    "Unable to resolve application"
            );
        }
        return result.getSingleResult();
    }
}
