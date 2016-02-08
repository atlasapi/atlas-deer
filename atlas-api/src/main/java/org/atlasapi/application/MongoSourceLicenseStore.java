package org.atlasapi.application;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.base.Optional;
import com.mongodb.DBCollection;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoSourceLicenseStore implements SourceLicenseStore {

    public static final String SOURCE_LICENSES_COLLECTION = "sourceLicenses";

    private SourceLicenseTranslator translator = new SourceLicenseTranslator();

    private final DBCollection sourceLicenses;

    public MongoSourceLicenseStore(DatabasedMongo mongo) {
        this.sourceLicenses = mongo.collection(SOURCE_LICENSES_COLLECTION);
    }

    @Override
    public Optional<SourceLicense> licenseFor(Publisher source) {
        return Optional.fromNullable(
                translator.fromDBObject(sourceLicenses.findOne(where().idEquals(source.key())
                        .build())));
    }

    @Override
    public void store(SourceLicense sourceLicense) {
        sourceLicenses.save(translator.toDBObject(sourceLicense));
    }

}
