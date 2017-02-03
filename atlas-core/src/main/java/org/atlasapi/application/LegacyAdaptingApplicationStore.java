package org.atlasapi.application;

import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.legacy.application.ApplicationTranslator;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.model.translators.ApplicationModelTranslator;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.text.MoreStrings;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.legacy.application.ApplicationConfigurationTranslator.PUBLISHER_KEY;
import static org.atlasapi.legacy.application.ApplicationConfigurationTranslator.SOURCES_KEY;
import static org.atlasapi.legacy.application.ApplicationConfigurationTranslator.STATE_KEY;
import static org.atlasapi.legacy.application.ApplicationConfigurationTranslator.WRITABLE_KEY;
import static org.atlasapi.legacy.application.ApplicationTranslator.APPLICATION_CONFIG_KEY;
import static org.atlasapi.legacy.application.ApplicationTranslator.DEER_ID_KEY;

public class LegacyAdaptingApplicationStore extends AbstractApplicationStore
        implements LegacyApplicationStore {

    private final ApplicationModelTranslator transformer;
    private final org.atlasapi.legacy.application.ApplicationStore legacyStore;
    private final DBCollection collection;
    private final ApplicationTranslator legacyTranslator;

    private final Function<DBObject, Application> legacyTranslatorFunction
            = new Function<DBObject, Application>() {

        @Override
        public Application apply(DBObject input) {
            return transformer.apply(legacyTranslator.fromDBObject(input));
        }
    };

    public LegacyAdaptingApplicationStore(org.atlasapi.legacy.application.ApplicationStore legacyStore,
            DatabasedMongo db,
            IdGenerator idGenerator, NumberToShortStringCodec idCodec) {
        super(idGenerator, idCodec);
        this.legacyStore = legacyStore;
        this.collection = db.collection("applications");
        this.transformer = new ApplicationModelTranslator();
        this.legacyTranslator = new ApplicationTranslator();
    }

    @Override
    void doCreateApplication(Application application) {
        legacyStore.persist(transformer.transform4to3(application));
    }

    @Override
    void doUpdateApplication(Application application) {
        legacyStore.update(transformer.transform4to3(application));
    }

    @Override
    public ListenableFuture<Resolved<Application>> resolveIds(Iterable<Id> ids) {
        Iterable<Long> idLongs = Iterables.transform(ids, Id.toLongValue());
        DBObject query = where().longFieldIn(ApplicationTranslator.DEER_ID_KEY, idLongs).build();
        DBCursor dbos = collection.find(query);
        Iterable<Application> apps = Iterables.transform(dbos, legacyTranslatorFunction);
        return Futures.immediateFuture(Resolved.valueOf(apps));
    }

    @Override
    public Optional<Application> applicationFor(Id id) {
        DBObject q = where().fieldEquals(DEER_ID_KEY, id.longValue()).build();
        DBObject dbo = collection.findOne(q);
        return dbo == null ? Optional.<Application>absent()
                           : Optional.of(legacyTranslatorFunction.apply(dbo));
    }

    @Override
    public Iterable<Application> allApplications() {
        return transformer.transform(legacyStore.allApplications());
    }

    @Override
    public Iterable<Application> readersFor(Publisher source) {
        String sourceField = String.format(
                "%s.%s.%s",
                APPLICATION_CONFIG_KEY,
                SOURCES_KEY,
                PUBLISHER_KEY
        );
        String stateField = String.format(
                "%s.%s.%s",
                APPLICATION_CONFIG_KEY,
                SOURCES_KEY,
                STATE_KEY
        );
        DBObject q = where().fieldEquals(sourceField, source.key())
                .fieldIn(stateField, states())
                .build();
        return Iterables.transform(collection.find(q), legacyTranslatorFunction);
    }

    private Iterable<String> states() {
        return Iterables.transform(
                ImmutableSet.of(SourceState.AVAILABLE, SourceState.REQUESTED),
                Functions.compose(MoreStrings.toLower(), Functions.toStringFunction())
        );
    }

    @Override
    public Iterable<Application> writersFor(Publisher source) {
        String sourceField = String.format("%s.%s", APPLICATION_CONFIG_KEY, WRITABLE_KEY);
        DBObject query = where().fieldEquals(sourceField, source.key()).build();
        return Iterables.transform(collection.find(query), legacyTranslatorFunction);
    }

    @Override
    public Optional<Application> applicationForKey(String apiKey) {
        return legacyStore.applicationForKey(apiKey).transform(transformer);
    }

    @Override
    public Optional<Id> applicationIdForSlug(String slug) {
        DBObject query = where().idEquals(slug).build();
        DBObject select = select().field(DEER_ID_KEY).build();
        DBObject dbo = collection.findOne(query, select);
        return Optional.fromNullable(dbo).transform(new Function<DBObject, Id>() {

            @Override
            public Id apply(DBObject input) {
                return Id.valueOf(TranslatorUtils.toLong(input, DEER_ID_KEY));
            }
        });
    }

    @Override
    public Iterable<Id> applicationIdsForSlugs(Iterable<String> slugs) {
        DBObject query = where().idIn(slugs).build();
        DBObject select = select().field(DEER_ID_KEY).build();
        DBCursor dbos = collection.find(query, select);
        return Iterables.transform(dbos, new Function<DBObject, Id>() {

            @Override
            public Id apply(DBObject input) {
                return Id.valueOf(TranslatorUtils.toLong(input, DEER_ID_KEY));
            }
        });
    }

}
