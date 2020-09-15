package org.atlasapi.query.common.attributes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.query.common.ParameterNameProvider;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class QueryAttributeParser implements ParameterNameProvider {

    private final Splitter valueSplitter = Splitter.on(',').omitEmptyStrings().trimResults();

    private final AttributeLookupTree attributesLookup;
    private final Map<Attribute<?>, ? extends QueryAtomParser<?>> parsers;
    private final Set<String> parameterNames;

    private QueryAttributeParser(Iterable<? extends QueryAtomParser<?>> attributeParsers) {
        this.parsers = Maps.uniqueIndex(attributeParsers, QueryAtomParser::getAttribute);
        this.attributesLookup = initLookup(parsers.keySet());
        this.parameterNames = attributesLookup.allKeys();
    }

    public static QueryAttributeParser create(
            Iterable<? extends QueryAtomParser<?>> attributeParsers
    ) {
        return new QueryAttributeParser(attributeParsers);
    }

    private AttributeLookupTree initLookup(Set<Attribute<?>> attributes) {
        AttributeLookupTree lookup = AttributeLookupTree.create();

        for (Attribute<?> attribute : attributes) {
            lookup.put(attribute);
        }

        return lookup;
    }

    public Set<AttributeQuery<?>> parse(HttpServletRequest request) throws QueryParseException {
        return parseListQuery(request);
    }

    @Override
    public Set<String> getOptionalParameters() {
        return parameterNames;
    }

    @Override
    public Set<String> getRequiredParameters() {
        return ImmutableSet.of();
    }

    private Set<AttributeQuery<?>> parseListQuery(HttpServletRequest request)
            throws QueryParseException {
        ImmutableSet.Builder<AttributeQuery<?>> operands = ImmutableSet.builder();

        for (Entry<String, String[]> param : getParameterMap(request).entrySet()) {
            Optional<Attribute<?>> attribute = attributesLookup.attributeFor(param.getKey());

            if (attribute.isPresent()) {
                QueryAtomParser<?> parser = parsers.get(attribute.get());

                for (String paramString : param.getValue()) {

                    Iterable<String> rawValues;
                    // Workaround specifically for the q parameter. Could use isCollectionOfValues
                    // on the Attribute object for a more general solution, however, the field has
                    // not been in use and thus every parameter is currently treated as a list, so
                    // as not to cause any unwanted side effects of correctly implementing the
                    // field, do this instead.
                    if (param.getKey().equals(Attributes.Q.externalName())) {
                        rawValues = Collections.singletonList(paramString);
                    } else {
                        rawValues = splitVals(paramString);
                    }

                    operands.add(parser.parse(param.getKey(), rawValues));
                }
            }
        }

        return operands.build();
    }

    private Iterable<String> splitVals(String value) {
        return FluentIterable.from(Arrays.asList(value))
                .transformAndConcat(valueSplitter::split);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String[]> getParameterMap(HttpServletRequest request) {
        return (Map<String, String[]>) request.getParameterMap();
    }
}
