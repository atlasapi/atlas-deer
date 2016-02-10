package org.atlasapi.application.auth;

import java.net.MalformedURLException;

import org.atlasapi.application.model.auth.OAuthRequest;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoOAuthRequestTranslator {

    public static final String NAMESPACE_KEY = "namespace";
    public static final String AUTH_URL_KEY = "authUrl";
    public static final String CALLBACK_URL_KEY = "callbackUrl";
    public static final String TOKEN_KEY = "token";
    public static final String SECRET_KEY = "secret";

    public DBObject toDBObject(OAuthRequest oauthRequest) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, oauthRequest.getUuid().toString());
        TranslatorUtils.from(dbo, NAMESPACE_KEY, oauthRequest.getUserNamespace().name());
        TranslatorUtils.from(dbo, AUTH_URL_KEY, oauthRequest.getAuthUrl().toExternalForm());
        TranslatorUtils.from(dbo, CALLBACK_URL_KEY, oauthRequest.getCallbackUrl().toExternalForm());
        TranslatorUtils.from(dbo, TOKEN_KEY, oauthRequest.getToken());
        TranslatorUtils.from(dbo, SECRET_KEY, oauthRequest.getSecret());
        return dbo;
    }

    public OAuthRequest fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        try {
            return OAuthRequest.builder()
                    .withUuid(TranslatorUtils.toString(dbo, MongoConstants.ID))
                    .withNamespace(UserNamespace.valueOf(TranslatorUtils.toString(
                            dbo,
                            NAMESPACE_KEY
                    )))
                    .withAuthUrl(TranslatorUtils.toString(dbo, AUTH_URL_KEY))
                    .withCallbackUrl(TranslatorUtils.toString(dbo, CALLBACK_URL_KEY))
                    .withToken(TranslatorUtils.toString(dbo, TOKEN_KEY))
                    .withSecret(TranslatorUtils.toString(dbo, SECRET_KEY))
                    .build();
        } catch (MalformedURLException e) {
            return null;
        }
    }

}
