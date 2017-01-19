package org.atlasapi.application;

import java.util.UUID;

import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.application.model.auth.TokenRequestStore;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.base.Optional;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoTokenRequestStore implements TokenRequestStore {

    public static final String TOKEN_REQUEST_COLLECTION = "tokenRequests";
    private final DBCollection tokenRequests;
    private final MongoOAuthRequestTranslator translator = new MongoOAuthRequestTranslator();

    public MongoTokenRequestStore(DatabasedMongo adminMongo) {
        this.tokenRequests = adminMongo.collection(TOKEN_REQUEST_COLLECTION);
        this.tokenRequests.setReadPreference(ReadPreference.primary());
    }

    @Override
    public void store(OAuthRequest oauthRequest) {
        tokenRequests.save(translator.toDBObject(oauthRequest));
    }

    @Override
    public Optional<OAuthRequest> lookupAndRemove(UserNamespace namespace, String token) {
        DBObject dbo = tokenRequests.findOne(where()
                .fieldEquals(MongoOAuthRequestTranslator.NAMESPACE_KEY, namespace.name())
                .fieldEquals(MongoOAuthRequestTranslator.TOKEN_KEY, token)
                .build());
        if (dbo != null) {
            OAuthRequest oauthRequest = translator.fromDBObject(dbo);
            tokenRequests.remove(dbo);
            return Optional.of(oauthRequest);
        } else {
            return Optional.absent();
        }
    }

    @Override
    public Optional<OAuthRequest> lookupAndRemove(UUID uuid) {
        DBObject dbo = tokenRequests.findOne(where()
                .idEquals(uuid.toString())
                .build());
        if (dbo != null) {
            OAuthRequest oauthRequest = translator.fromDBObject(dbo);
            tokenRequests.remove(dbo);
            return Optional.of(oauthRequest);
        } else {
            return Optional.absent();
        }
    }

}
