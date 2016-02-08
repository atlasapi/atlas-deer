package org.atlasapi.users.videosource.model.translators;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.users.videosource.model.UserVideoSource;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.social.model.translator.UserRefTranslator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class UserVideoSourceTranslator {

    public static final String PUBLISHER_KEY = "publisher";
    public static final String TOKEN_KEY = "token";
    public static final String CHANNEL_IDS_KEYS = "channelIds";
    public static final String NAME_KEY = "name";
    public static final String ATLAS_USER_KEY = "atlasUser";
    public static final String USER_REF_KEY = "userRef";
    private UserRefTranslator userRefTranslator = new UserRefTranslator();
    private OauthTokenTranslator tokenDetailsTranslator = new OauthTokenTranslator();

    public DBObject toDBObject(UserVideoSource tokenUser) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, tokenUser.getUserRef().toKey());
        TranslatorUtils.from(
                dbo,
                USER_REF_KEY,
                userRefTranslator.toDBObject(tokenUser.getUserRef())
        );
        TranslatorUtils.from(dbo, ATLAS_USER_KEY, tokenUser.getAtlasUser().longValue());
        TranslatorUtils.from(dbo, NAME_KEY, tokenUser.getName());
        TranslatorUtils.fromIterable(dbo, tokenUser.getChannelIds(), CHANNEL_IDS_KEYS);
        TranslatorUtils.from(
                dbo,
                TOKEN_KEY,
                tokenDetailsTranslator.toDBObject(tokenUser.getToken())
        );
        TranslatorUtils.from(dbo, PUBLISHER_KEY, tokenUser.getPublisher().name());
        return dbo;
    }

    public UserVideoSource fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return UserVideoSource.builder()
                .withUserRef(userRefTranslator.fromDBObject(TranslatorUtils.toDBObject(
                        dbo,
                        USER_REF_KEY
                )))
                .withAtlasUser(Id.valueOf(TranslatorUtils.toLong(dbo, ATLAS_USER_KEY)))
                .withName(TranslatorUtils.toString(dbo, NAME_KEY))
                .withChannelIds(TranslatorUtils.toSet(dbo, CHANNEL_IDS_KEYS))
                .withToken(tokenDetailsTranslator.fromDBObject(TranslatorUtils.toDBObject(
                        dbo,
                        TOKEN_KEY
                )))
                .withPublisher(Publisher.valueOf(TranslatorUtils.toString(dbo, PUBLISHER_KEY)))
                .build();
    }

}
