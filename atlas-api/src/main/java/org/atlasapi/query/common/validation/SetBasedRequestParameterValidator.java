package org.atlasapi.query.common.validation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

public class SetBasedRequestParameterValidator extends AbstractRequestParameterValidator {

    private final Set<String> requiredParams;
    private final Set<String> optionalParams;
    private final Set<String> requiredAlternativeParams;

    private final Set<String> allParams;
    private final String validParamMsg;
    private final ReplacementSuggestion replacementSuggestion;

    private SetBasedRequestParameterValidator(
            Set<String> requiredParams,
            Set<String> optionalParams,
            Set<String> requiredAlternativeParams
    ) {
        this.requiredParams = ImmutableSet.copyOf(requiredParams);
        this.optionalParams = Sets.union(optionalParams, ALWAYS_OPTIONAL_PARAMS).immutableCopy();
        this.requiredAlternativeParams = ImmutableSet.copyOf(requiredAlternativeParams);
        this.allParams = ImmutableSet.copyOf(
                Sets.union(
                        Sets.union(this.requiredParams, this.optionalParams),
                        this.requiredAlternativeParams
                )
        );
        this.validParamMsg = "Valid params: " + commaJoiner.join(allParams);
        this.replacementSuggestion = ReplacementSuggestion.create(
                allParams,
                "Invalid parameters: ",
                " (did you mean %s?)"
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Set<String> determineMissingParameters(Set<String> requestParams) {
        return Sets.difference(requiredParams, requestParams);
    }

    @Override
    protected Collection<String> determineConflictingParameters(Set<String> requestParams) {
        if (requiredAlternativeParams.size() < 2) {
            return ImmutableSet.of();
        }

        Collection<String> requestRequiredAlternativeParams = Sets.intersection(
                requiredAlternativeParams,
                requestParams
        );
        if (requestRequiredAlternativeParams.isEmpty()
                || requestRequiredAlternativeParams.size() > 1) {
            return requiredAlternativeParams;
        }
        return ImmutableList.of();
    }

    @Override
    protected Set<String> determineInvalidParameters(Set<String> requestParams) {
        return Sets.difference(requestParams, allParams);
    }

    @Override
    protected String missingParameterMessage(Collection<String> missingParams) {
        return String.format("Missing parameters: %s.", commaJoiner.join(missingParams));
    }

    @Override
    protected String conflictingParameterMessage(Collection<String> conflictingParams) {
        return String.format("You must specify ONE of: %s.", commaJoiner.join(conflictingParams));
    }

    @Override
    protected String invalidParameterMessage(Collection<String> invalidParams) {
        return String.format(
                "%s. %s.",
                replacementSuggestion.forInvalid(invalidParams),
                validParamMsg
        );
    }

    public static final class Builder {

        private ImmutableSet<String> required;
        private ImmutableSet<String> optional;
        private ImmutableSet<String> requiredAlternatives = ImmutableSet.of();

        private Builder() {
        }

        public Builder withRequiredParameters(String... parameters) {
            this.required = ImmutableSet.copyOf(parameters);
            return this;
        }

        public Builder withOptionalParameters(String... parameters) {
            this.optional = ImmutableSet.copyOf(parameters);
            return this;
        }

        public Builder withRequiredAlternativeParameters(String... parameters) {
            this.requiredAlternatives = ImmutableSet.copyOf(parameters);
            return this;
        }

        public SetBasedRequestParameterValidator build() {
            return new SetBasedRequestParameterValidator(required, optional, requiredAlternatives);
        }
    }
}
