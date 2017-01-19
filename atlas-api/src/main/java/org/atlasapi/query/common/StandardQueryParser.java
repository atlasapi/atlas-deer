package org.atlasapi.query.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.InvalidApiKeyException;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.context.QueryContextParser;
import org.atlasapi.query.common.exceptions.InvalidIdentifierException;
import org.atlasapi.query.common.validation.AbstractRequestParameterValidator;
import org.atlasapi.query.common.validation.QueryRequestParameterValidator;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class StandardQueryParser<T> implements QueryParser<T> {

    private final NumberToShortStringCodec idCodec;
    private final QueryAttributeParser attributeParser;
    private final QueryContextParser contextParser;

    private final Pattern singleResourcePattern;
    private final AbstractRequestParameterValidator parameterValidator;

    private StandardQueryParser(
            Resource resource,
            QueryAttributeParser attributeParser,
            NumberToShortStringCodec idCodec,
            QueryContextParser contextParser
    ) {
        this.parameterValidator = QueryRequestParameterValidator.create(
                attributeParser,
                contextParser
        );
        this.attributeParser = checkNotNull(attributeParser);
        this.contextParser = checkNotNull(contextParser);
        this.idCodec = checkNotNull(idCodec);
        this.singleResourcePattern = Pattern.compile(resource.getPlural() + "/([^.]+)(\\..*)?$");
    }

    public static <T> StandardQueryParser<T> create(
            Resource resource,
            QueryAttributeParser attributeParser,
            NumberToShortStringCodec idCodec,
            QueryContextParser contextParser
    ) {
        return new StandardQueryParser<>(resource, attributeParser, idCodec, contextParser);
    }

    @Override
    public Query<T> parse(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        parameterValidator.validateParameters(request);
        Id singleId = tryExtractSingleId(request);

        return singleId != null
               ? singleQuery(request, singleId)
               : listQuery(request);
    }

    @Nullable
    private Id tryExtractSingleId(HttpServletRequest request) throws QueryParseException {
        Matcher matcher = singleResourcePattern.matcher(request.getRequestURI());
        try {
            return matcher.find()
                   ? Id.valueOf(idCodec.decode(matcher.group(1)))
                   : null;
        } catch (IllegalArgumentException e) {
            throw new InvalidIdentifierException(e.getMessage(), e);
        }
    }

    private Query<T> singleQuery(HttpServletRequest request, Id singleId)
            throws QueryParseException, InvalidApiKeyException {
        return Query.singleQuery(singleId, contextParser.parseSingleContext(request));
    }

    private Query<T> listQuery(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        AttributeQuerySet querySet = attributeParser.parse(request);
        return Query.listQuery(
                querySet,
                contextParser.parseListContext(request)
        );
    }
}
