package org.atlasapi.query.common.useraware;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.content.QueryParseException;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.query.common.AbstractRequestParameterValidator;
import org.atlasapi.query.common.QueryAttributeParser;
import org.atlasapi.query.common.QueryRequestParameterValidator;
import org.atlasapi.query.common.Resource;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import static com.google.common.base.Preconditions.checkNotNull;

public class StandardUserAwareQueryParserNoAuth<T> implements UserAccountsAwareQueryParser<T> {

    private final NumberToShortStringCodec idCodec;
    private final QueryAttributeParser attributeParser;
    private final UserAwareQueryContextParserNoAuth contextParser;

    private final Pattern singleResourcePattern;
    private final AbstractRequestParameterValidator parameterValidator;

    public StandardUserAwareQueryParserNoAuth(Resource resource, QueryAttributeParser attributeParser,
            NumberToShortStringCodec idCodec,
            UserAwareQueryContextParserNoAuth contextParser) {
        this.parameterValidator = new QueryRequestParameterValidator(
                attributeParser,
                contextParser
        );
        this.attributeParser = checkNotNull(attributeParser);
        this.contextParser = checkNotNull(contextParser);
        this.idCodec = checkNotNull(idCodec);
        this.singleResourcePattern = Pattern.compile(resource.getPlural() + "/([^.]+)(\\..*)?$");
    }

    @Override
    public UserAccountsAwareQuery<T> parse(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        parameterValidator.validateParameters(request);
        Id singleId = tryExtractSingleId(request);
        return singleId != null ? singleQuery(request, singleId)
                                : listQuery(request);
    }

    private Id tryExtractSingleId(HttpServletRequest request) {
        Matcher matcher = singleResourcePattern.matcher(request.getRequestURI());
        return matcher.find() ? Id.valueOf(idCodec.decode(matcher.group(1)))
                              : null;
    }

    private UserAccountsAwareQuery<T> singleQuery(HttpServletRequest request, Id singleId)
            throws QueryParseException, InvalidApiKeyException {
        return UserAccountsAwareQuery.singleQuery(singleId, contextParser.parseSingleContext(request));
    }

    private UserAccountsAwareQuery<T> listQuery(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException {
        AttributeQuerySet querySet = attributeParser.parse(request);
        return UserAccountsAwareQuery.listQuery(
                querySet,
                contextParser.parseListContext(request)
        );
    }

}