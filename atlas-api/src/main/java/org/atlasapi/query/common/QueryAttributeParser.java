package org.atlasapi.query.common;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attribute;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class QueryAttributeParser implements ParameterNameProvider {
    
    private final Splitter valueSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
    
    private final AttributeLookupTree attributesLookup;
    private final Map<Attribute<?>, ? extends QueryAtomParser<String, ?>> parsers;
    private final Set<String> parameterNames;

    public QueryAttributeParser(Iterable<? extends QueryAtomParser<String, ?>> attributeParsers) {
        this.parsers = Maps.uniqueIndex(attributeParsers, QueryAtomParser::getAttribute);
        this.attributesLookup = initLookup(parsers.keySet());
        this.parameterNames = attributesLookup.allKeys();
    }

    private AttributeLookupTree initLookup(Set<Attribute<?>> attributes) {
        AttributeLookupTree lookup = new AttributeLookupTree();
        for (Attribute<?> attribute : attributes) {
            lookup.put(attribute);
        }
        return lookup;
    }

    public AttributeQuerySet parse(HttpServletRequest request) throws QueryParseException {
        return new AttributeQuerySet(parseListQuery(request));
    }

    private Iterable<? extends AttributeQuery<?>> parseListQuery(HttpServletRequest request)
            throws QueryParseException {
        ImmutableSet.Builder<AttributeQuery<?>> operands = ImmutableSet.builder();
        for(Entry<String, String[]> param : getParameterMap(request).entrySet()) {
            Optional<Attribute<?>> attribute = attributesLookup.attributeFor(param.getKey());
            if (attribute.isPresent()) {
                QueryAtomParser<String, ?> parser = parsers.get(attribute.get());
                operands.add(parser.parse(param.getKey(), splitVals(param.getValue())));
            }
        }
        return operands.build();
    }

    private Iterable<String> splitVals(String[] value) {
        return FluentIterable.from(Arrays.asList(value))
            .transformAndConcat(valueSplitter::split);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String[]> getParameterMap(HttpServletRequest request) {
        return (Map<String, String[]>) request.getParameterMap();
    }

    @Override
    public Set<String> getOptionalParameters() {
        return parameterNames;
    }
    
    @Override
    public Set<String> getRequiredParameters() {
        return ImmutableSet.of();
    }

}
