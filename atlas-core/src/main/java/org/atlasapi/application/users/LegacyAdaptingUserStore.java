package org.atlasapi.application.users;

import org.atlasapi.application.LegacyApplicationStore;
import org.atlasapi.application.users.v3.UserTranslator;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.model.translators.UserModelTranslator;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.translator.UserRefTranslator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class LegacyAdaptingUserStore implements UserStore {

    private final org.atlasapi.application.users.v3.UserStore legacyStore;
    private final UserModelTranslator transformer;
    private final DBCollection collection;
    private final UserTranslator translator;

    private final Function<DBObject, User> legacyTranslatorFunction
            = new Function<DBObject, User>() {

        @Override
        public User apply(DBObject input) {
            return transformer.apply(translator.fromDBObject(input));
        }
    };

    public LegacyAdaptingUserStore(org.atlasapi.application.users.v3.UserStore legacyStore,
            LegacyApplicationStore appStore, DatabasedMongo db) {
        this.legacyStore = legacyStore;
        this.collection = db.collection("users");
        this.transformer = new UserModelTranslator(appStore);
        this.translator = new UserTranslator(new UserRefTranslator());
    }

    @Override
    public ListenableFuture<Resolved<User>> resolveIds(Iterable<Id> ids) {
        Iterable<Long> lids = Iterables.transform(ids, Id.toLongValue());
        DBObject query = MongoBuilders.where().longIdIn(lids).build();
        Iterable<User> users = Iterables.transform(
                collection.find(query),
                legacyTranslatorFunction
        );
        return Futures.immediateFuture(Resolved.valueOf(users));
    }

    @Override
    public Optional<User> userForRef(UserRef ref) {
        return legacyStore.userForRef(ref).transform(transformer);
    }

    @Override
    public Optional<User> userForId(Id id) {
        return legacyStore.userForId(id.longValue()).transform(transformer);
    }

    @Override
    public Iterable<User> allUsers() {
        return Iterables.transform(collection.find(), legacyTranslatorFunction);
    }

    @Override
    public void store(User user) {
        legacyStore.store(transformer.transform4to3(user));
    }

}
