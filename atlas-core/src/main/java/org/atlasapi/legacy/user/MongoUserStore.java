package org.atlasapi.legacy.user;

import java.util.Set;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

import com.google.common.base.Optional;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.translator.UserRefTranslator;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoUserStore implements UserStore {

    public static final String EMAIL = "email";
    private DBCollection users;
    private UserTranslator translator;
    private UserRefTranslator userRefTranslator;

    public MongoUserStore(DatabasedMongo mongo) {
        this.users = mongo.collection("users");
        this.userRefTranslator = new UserRefTranslator();
        this.translator = new UserTranslator(userRefTranslator);
    }

    @Override
    public Optional<User> userForRef(UserRef ref) {
        return Optional.fromNullable(translator.fromDBObject(users.findOne(userRefTranslator.toQuery(ref, "userRef").build())));
    }

    @Override
    public Optional<User> userForId(Long userId) {
        return Optional.fromNullable(translator.fromDBObject(users.findOne(userId)));
    }

    @Override
    public Set<User> userAccountsForEmail(String email) {
        BasicDBObject emailField = new BasicDBObject();
        emailField.append(EMAIL, email);
        DBCursor dbObjects = users.find(emailField);
        Set<User> userAccounts = Sets.newHashSet();
        for (DBObject dbObject : dbObjects) {
            userAccounts.add(translator.fromDBObject(dbObject));
        }
        return userAccounts;
    }

    @Override
    public void store(User user) {
        store(translator.toDBObject(user));
    }

    public void store(final DBObject dbo) {
        this.users.update(new BasicDBObject(ID, dbo.get(ID)), dbo, UPSERT, SINGLE);
    }

}