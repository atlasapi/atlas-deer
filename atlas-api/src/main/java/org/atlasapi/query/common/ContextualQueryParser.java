package org.atlasapi.query.common;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationResolutionException;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.Query.ListQuery;
import org.atlasapi.query.common.Query.SingleQuery;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.validation.AbstractRequestParameterValidator;
import org.atlasapi.query.common.validation.QueryRequestParameterValidator;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContextualQueryParser<C, R> {

    private final NumberToShortStringCodec idCodec;
    private final Attribute<Id> contextResouceAttribute;
    private final ContextualQueryContextParser queryContextParser;
    private final QueryAttributeParser attributeParser;

    private final AbstractRequestParameterValidator parameterValidator;
    private final Pattern contextResourcePattern;

    public ContextualQueryParser(Resource context,
            Attribute<Id> contextResouceAttribute,
            Resource query,
            NumberToShortStringCodec idCodec,
            QueryAttributeParser attributeParser, ContextualQueryContextParser contextParser) {
        this.attributeParser = checkNotNull(attributeParser);
        this.queryContextParser = checkNotNull(contextParser);
        this.contextResouceAttribute = checkNotNull(contextResouceAttribute);
        this.idCodec = checkNotNull(idCodec);
        this.parameterValidator = QueryRequestParameterValidator.create(
                attributeParser,
                contextParser
        );
        this.contextResourcePattern = contextResourcePattern(query, context);
    }

    private Pattern contextResourcePattern(Resource query, Resource context) {
        return Pattern.compile(context.getPlural() + "/([^/]+)/" + query.getPlural() + "(\\..*)?$");
    }

    public ContextualQuery<C, R> parse(HttpServletRequest request)
            throws QueryParseException, ApplicationResolutionException {
        parameterValidator.validateParameters(request);
        QueryContext context = queryContextParser.parseContext(request);
        SingleQuery<C> contextQuery = contextQuery(request, context);
        return new ContextualQuery<C, R>(
                contextQuery, resourceQuery(request, contextQuery.getOnlyId(), context), context);
    }

    private ListQuery<R> resourceQuery(
            HttpServletRequest request,
            Id contextId,
            QueryContext context
    )
            throws QueryParseException {
        Set<AttributeQuery<?>> querySet = attributeParser.parse(request);
        querySet = ImmutableSet.<AttributeQuery<?>>builder()
                .addAll(querySet)
                .add(contextAttributeQuery(contextId))
                .build();
        return Query.listQuery(querySet, context);
    }

    private AttributeQuery<Id> contextAttributeQuery(Id contextId) {
        return contextResouceAttribute.createQuery(Operators.EQUALS, ImmutableList.of(contextId));
    }

    private SingleQuery<C> contextQuery(HttpServletRequest request, QueryContext context) {
        return Query.singleQuery(contextId(request), context);
    }

    private Id contextId(HttpServletRequest request) {
        Matcher matcher = contextResourcePattern.matcher(request.getRequestURI());
        if (matcher.find()) {
            return Id.valueOf(idCodec.decode(matcher.group(1)));
        }
        throw new IllegalArgumentException(contextResourcePattern
                + " couldn't extract context ID from " + request.getRequestURI());
    }

}
