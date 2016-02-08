package org.atlasapi.users.videosource.model.translators;

import org.atlasapi.users.videosource.model.OauthToken;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class OauthTokenTranslator {

    private static final String REFRESH_TOKEN_KEY = "refreshToken";
    private static final String ID_TOKEN_KEY = "idToken";
    private static final String EXPIRES_IN_KEY = "expiresIn";
    private static final String TOKEN_TYPE_KEY = "tokenType";
    private static final String ACCESS_TOKEN_KEY = "accessToken";

    public DBObject toDBObject(OauthToken token) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, ACCESS_TOKEN_KEY, token.getAccessToken());
        TranslatorUtils.from(dbo, TOKEN_TYPE_KEY, token.getTokenType());
        TranslatorUtils.from(dbo, EXPIRES_IN_KEY, token.getExpiresIn());
        TranslatorUtils.from(dbo, ID_TOKEN_KEY, token.getIdToken());
        TranslatorUtils.from(dbo, REFRESH_TOKEN_KEY, token.getRefreshToken());
        return dbo;
    }

    public OauthToken fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return OauthToken.builder()
                .withAccessToken(TranslatorUtils.toString(dbo, ACCESS_TOKEN_KEY))
                .withTokenType(TranslatorUtils.toString(dbo, TOKEN_TYPE_KEY))
                .withExpiresIn(TranslatorUtils.toLong(dbo, EXPIRES_IN_KEY))
                .withIdToken(TranslatorUtils.toString(dbo, ID_TOKEN_KEY))
                .withRefreshToken(TranslatorUtils.toString(dbo, REFRESH_TOKEN_KEY))
                .build();
    }
}
