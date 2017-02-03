package org.atlasapi.legacy.application;

import org.joda.time.DateTime;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ApplicationTranslator {

    public static final String APPLICATION_SLUG_KEY = MongoConstants.ID;
    public static final String APPLICATION_TITLE_KEY = "title";
    public static final String APPLICATION_DESCRIPTION_KEY = "desc";
    public static final String APPLICATION_CREATED_KEY = "created";
    public static final String APPLICATION_LAST_UPDATED_KEY = "lastUpdated";
    public static final String APPLICATION_CONFIG_KEY = "configuration";
    public static final String APPLICATION_CREDENTIALS_KEY = "credentials";
    public static final String DEER_ID_KEY = "aid";
    public static final String REVOKED_KEY = "revoked";
    public static final String NUMBER_OF_USERS_KEY = "numberOfUsers";
    public static final String STRIPE_CUSTOMER_ID_KEY = "stripeCustomerId";

    private final ApplicationConfigurationTranslator configurationTranslator = new ApplicationConfigurationTranslator();
    private final ApplicationCredentialsTranslator credentialsTranslator = new ApplicationCredentialsTranslator();

    public DBObject toDBObject(Application application) {
        DBObject dbo = new BasicDBObject();

        if (application != null) {
            TranslatorUtils.from(dbo, APPLICATION_SLUG_KEY, application.getSlug());
            TranslatorUtils.from(dbo, APPLICATION_TITLE_KEY, application.getTitle());
            TranslatorUtils.from(dbo, APPLICATION_DESCRIPTION_KEY, application.getDescription());
            TranslatorUtils.fromDateTime(dbo, APPLICATION_CREATED_KEY, application.getCreated());
            TranslatorUtils.fromDateTime(dbo, APPLICATION_LAST_UPDATED_KEY, application.getLastUpdated());
            TranslatorUtils.from(dbo, APPLICATION_CONFIG_KEY, configurationTranslator.toDBObject(application.getConfiguration()));
            TranslatorUtils.from(dbo, APPLICATION_CREDENTIALS_KEY, credentialsTranslator.toDBObject(application.getCredentials()));
            TranslatorUtils.from(dbo, DEER_ID_KEY, application.getDeerId());
            TranslatorUtils.from(dbo, REVOKED_KEY, application.isRevoked());
            TranslatorUtils.from(dbo, NUMBER_OF_USERS_KEY, application.getNumberOfUsers());
            TranslatorUtils.from(dbo, STRIPE_CUSTOMER_ID_KEY, application.getStripeCustomerId().orNull());
        }
        return dbo;
    }

    public Application fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }

        String applicationSlug = TranslatorUtils.toString(dbo, APPLICATION_SLUG_KEY);
        if(applicationSlug == null){
            return null;
        }

        boolean revoked = false;
        if (dbo.containsField(REVOKED_KEY)) {
            revoked = TranslatorUtils.toBoolean(dbo, REVOKED_KEY);
        }

        DateTime lastUpdated = null;
        if (dbo.containsField(APPLICATION_LAST_UPDATED_KEY)) {
            lastUpdated = TranslatorUtils.toDateTime(dbo, APPLICATION_LAST_UPDATED_KEY);
        }

        Long numberOfUsers = Long.valueOf(1L);
        if (dbo.containsField(NUMBER_OF_USERS_KEY)) {
            numberOfUsers = TranslatorUtils.toLong(dbo, NUMBER_OF_USERS_KEY);
        }

        return Application.application(applicationSlug)
                .withTitle(TranslatorUtils.toString(dbo, APPLICATION_TITLE_KEY))
                .withDescription(TranslatorUtils.toString(dbo, APPLICATION_DESCRIPTION_KEY))
                .createdAt(TranslatorUtils.toDateTime(dbo, APPLICATION_CREATED_KEY))
                .withLastUpdated(lastUpdated)
                .withConfiguration(configurationTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo, APPLICATION_CONFIG_KEY)))
                .withCredentials(credentialsTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo, APPLICATION_CREDENTIALS_KEY)))
                .withDeerId(TranslatorUtils.toLong(dbo, DEER_ID_KEY))
                .withRevoked(revoked)
                .withNumberOfUsers(numberOfUsers)
                .withStripeCustomerId(TranslatorUtils.toString(dbo, STRIPE_CUSTOMER_ID_KEY))
                .build();
    }

}