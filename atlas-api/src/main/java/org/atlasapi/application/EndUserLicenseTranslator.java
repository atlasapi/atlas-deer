package org.atlasapi.application;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class EndUserLicenseTranslator {
    public static final String LICENSE_KEY = "license";
    
    public DBObject toDBObject(EndUserLicense endUserLicense) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, endUserLicense.getId().longValue());
        TranslatorUtils.from(dbo, LICENSE_KEY, endUserLicense.getLicense());
        return dbo;
    }
    
    public EndUserLicense fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return EndUserLicense.builder()
                .withId(Id.valueOf(TranslatorUtils.toLong(dbo, MongoConstants.ID)))
                .withLicense(TranslatorUtils.toString(dbo, LICENSE_KEY))
                .build();
    }
}
