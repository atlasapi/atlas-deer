package org.atlasapi.neo4j.service;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.query.ActionableEpisodesQuery;
import org.atlasapi.neo4j.service.query.GraphQuery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ContentGraphServiceSelector {

    private static final ImmutableSet<String> ACTIONABLE_EPISODES_REQUIRED_PARAMETERS =
            ImmutableSet.of(
                    "actionableFilterParameters",
                    "type",
                    "series.id"
            );
    private static final Map<String, String> ACTIONABLE_EPISODES_REQUIRED_PARAMETERS_WITH_VALUES =
            ImmutableMap.of(
                    "type", "episode"
            );
    private static final Set<String> ACTIONABLE_EPISODES_OPTIONAL_SUPPORTED_PARAMETERS =
            ImmutableSet.of(
                    "key",
                    "order_by",
                    "annotations",
                    "limit",
                    "region"
            );
    private static final ImmutableSet<String> ALLOWED_PARAMETERS =
            ImmutableSet.<String>builder()
                    .addAll(ACTIONABLE_EPISODES_REQUIRED_PARAMETERS)
                    .addAll(ACTIONABLE_EPISODES_REQUIRED_PARAMETERS_WITH_VALUES.keySet())
                    .addAll(ACTIONABLE_EPISODES_OPTIONAL_SUPPORTED_PARAMETERS)
                    .build();

    private ContentGraphServiceSelector() { }

    public static ContentGraphServiceSelector create() {
        return new ContentGraphServiceSelector();
    }

    public Optional<GraphQuery> getGraphQuery(
            IndexQueryParams indexQueryParams,
            Iterable<Publisher> publishers,
            Map<String, String> requestParameters
    ) {
        if (supportsQueryParameters(requestParameters)) {
            return Optional.of(ActionableEpisodesQuery.create(
                    indexQueryParams, publishers
            ));
        }

        return Optional.empty();
    }

    private boolean supportsQueryParameters(Map<String, String> parameters) {
        return checkRequiredParameters(parameters.keySet())
                && checkRequiredParametersWithValues(parameters)
                && checkAllowedParameters(parameters.keySet());
    }

    private boolean checkRequiredParameters(Set<String> parameters) {
        return parameters.containsAll(ACTIONABLE_EPISODES_REQUIRED_PARAMETERS);
    }

    private boolean checkRequiredParametersWithValues(Map<String, String> parameters) {
        return ACTIONABLE_EPISODES_REQUIRED_PARAMETERS_WITH_VALUES.entrySet().stream()
                .allMatch(entry -> parameters.get(entry.getKey()).equals(entry.getValue()));
    }

    private boolean checkAllowedParameters(Set<String> parameters) {
        return parameters.stream()
                .noneMatch(parameter -> !ALLOWED_PARAMETERS.contains(parameter));
    }
}
