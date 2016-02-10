package org.atlasapi.users.videosource;

import org.atlasapi.application.users.User;
import org.atlasapi.users.videosource.model.UserVideoSource;
import org.atlasapi.users.videosource.model.translators.UserVideoSourceTranslator;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.social.model.translator.UserRefTranslator.USER_NAMESPACE_KEY;
import static org.atlasapi.users.videosource.model.translators.UserVideoSourceTranslator.ATLAS_USER_KEY;
import static org.atlasapi.users.videosource.model.translators.UserVideoSourceTranslator.USER_REF_KEY;

public class MongoUserVideoSourceStore implements
        UserVideoSourceStore {

    private static final String COLLECTION_NAME = "userVideoSources";
    private static final String NAMESPACE_KEY = USER_REF_KEY + "." + USER_NAMESPACE_KEY;
    private final DBCollection linkedTokens;
    private UserVideoSourceTranslator translator = new UserVideoSourceTranslator();

    private final Function<DBObject, UserVideoSource> translatorFunction = new Function<DBObject, UserVideoSource>() {

        @Override
        public UserVideoSource apply(DBObject dbo) {
            return translator.fromDBObject(dbo);
        }
    };

    public MongoUserVideoSourceStore(DatabasedMongo mongo) {
        this.linkedTokens = mongo.collection(COLLECTION_NAME);
    }

    @Override
    public void store(UserVideoSource userVideoSource) {
        linkedTokens.save(translator.toDBObject(userVideoSource));
    }

    @Override
    public Iterable<UserVideoSource> userVideoSourcesFor(User user) {
        return ImmutableSet.copyOf(Iterables.transform(
                linkedTokens.find(where().fieldEquals(ATLAS_USER_KEY, user.getId().longValue())
                        .build()),
                translatorFunction
        ));
    }

    @Override
    public Iterable<UserVideoSource> userVideoSourcesFor(User user, UserNamespace videoService) {
        return ImmutableSet.copyOf(Iterables.transform(
                linkedTokens.find(where().fieldEquals(ATLAS_USER_KEY, user.getId().longValue())
                        .fieldEquals(NAMESPACE_KEY, videoService.prefix()).build()),
                translatorFunction
        ));
    }

    @Override
    public UserVideoSource sourceForRemoteuserRef(UserRef userRef) {
        return translator.fromDBObject(linkedTokens.findOne(where().idEquals(userRef.toKey())
                .build()));
    }

}
