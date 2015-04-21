package org.atlasapi.query.common;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


public class QueryRequestParameterValidator extends AbstractRequestParameterValidator {

    private final PrefixInTree<Boolean> attributeParameters;
    private final ImmutableSet<String> requiredParameters;
    private final ImmutableSet<String> optionalParameters;
    private final ReplacementSuggestion replacementSuggestion;


    public QueryRequestParameterValidator(QueryAttributeParser attributeParser,
            ParameterNameProvider paramProvider) {
        this(attributeParser, paramProvider.getRequiredParameters(), paramProvider.getOptionalParameters());
    }
    
    public QueryRequestParameterValidator(QueryAttributeParser attributeParser,
            Set<String> requiredParameters, Set<String> optionalParameters) {
        this.attributeParameters = initAttributeParams(attributeParser);
        this.requiredParameters = ImmutableSet.copyOf(requiredParameters);
        this.optionalParameters = ImmutableSet.copyOf(optionalParameters);
        this.replacementSuggestion = new ReplacementSuggestion(allParams(), "Invalid parameters: ", " (did you mean %s?)");
    }
    

    private Iterable<String> allParams() {
        return ImmutableSet.copyOf(Iterables.concat(
                attributeParameters.allKeys(), requiredParameters, optionalParameters));
    }

    private PrefixInTree<Boolean> initAttributeParams(QueryAttributeParser attributeParser) {
        PrefixInTree<Boolean> attributeParams = new PrefixInTree<Boolean>();
        Optional<Boolean> value = Optional.of(Boolean.TRUE);
        for (String validKeyPrefix : attributeParser.getOptionalParameters()) {
            attributeParams.put(validKeyPrefix, value);
        }
        return attributeParams;
    }

    @Override
    protected List<String> determineInvalidParameters(Set<String> requestParams) {
        LinkedList<String> invalid = Lists.newLinkedList();
        for (String requestParam : requestParams) {
            if (!(isContextParam(requestParam) || isAttributeParam(requestParam)))  {
                invalid.add(requestParam);
            }
        }
        return invalid;
    }

    private boolean isAttributeParam(String requestParam) {
        return attributeParameters.valueForKeyPrefixOf(requestParam).isPresent();
    }

    private boolean isContextParam(String requestParam) {
        return requiredParameters.contains(requestParam) || optionalParameters.contains(requestParam);
    }

    @Override
    protected Set<String> determineMissingParameters(Set<String> requestParams) {
        return Sets.filter(requiredParameters, not(in(requestParams)));
    }

    @Override
    protected Collection<String> determineConflictingParameters(Set<String> requestParams) {
        return ImmutableSet.of();
    }

    @Override
    protected String invalidParameterMessage(Collection<String> invalidParams) {
        return replacementSuggestion.forInvalid(invalidParams);
    }

    @Override
    protected String missingParameterMessage(Collection<String> missingParams) {
        return Joiner.on(",").appendTo(new StringBuilder("Missing parameters: "), missingParams).toString();
    }

    @Override
    protected String conflictingParameterMessage(Collection<String> conflictingParams) {
        throw new UnsupportedOperationException("QueryRequestParameterValidator doesn't support alternative parameters(for now)");
    }

}
