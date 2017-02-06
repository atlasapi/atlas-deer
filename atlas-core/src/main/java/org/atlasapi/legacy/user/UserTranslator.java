package org.atlasapi.legacy.user;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.social.model.translator.UserRefTranslator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class UserTranslator {

    private static final String LICENSE_ACCEPTED_KEY = "licenseAccepted";
    private static final String PROFILE_COMPLETE_KEY = "profileComplete";
    private static final String PROFILE_IMAGE_KEY = "profileImage";
    private static final String WEBSITE_KEY = "website";
    private static final String EMAIL_KEY = "email";
    private static final String COMPANY_KEY = "company";
    private static final String FULL_NAME_KEY = "fullName";
    private static final String SCREEN_NAME_KEY = "screenName";
    private static final String ROLE_KEY = "role";
    private static final String MANAGES_KEY = "manages";
    private static final String APPS_KEY = "apps";
    private static final String USER_REF_KEY = "userRef";
    private static final String PROFILE_DEACTIVATED_KEY = "profileDeactivated";
    private final UserRefTranslator userTranslator;

    public UserTranslator(UserRefTranslator userTranslator) {
        this.userTranslator = userTranslator;
    }

    public DBObject toDBObject(User user) {
        if (user == null) {
            return null;
        }

        BasicDBObject dbo = new BasicDBObject();

        TranslatorUtils.from(dbo, MongoConstants.ID, user.getId());
        if (user.getUserRef() != null) {
            TranslatorUtils.from(dbo, USER_REF_KEY, userTranslator.toDBObject(user.getUserRef()));
        }
        TranslatorUtils.from(dbo, SCREEN_NAME_KEY, user.getScreenName());
        TranslatorUtils.from(dbo, FULL_NAME_KEY, user.getFullName());
        TranslatorUtils.from(dbo, COMPANY_KEY, user.getCompany());
        TranslatorUtils.from(dbo, EMAIL_KEY, user.getEmail());
        TranslatorUtils.from(dbo, WEBSITE_KEY, user.getWebsite());
        TranslatorUtils.from(dbo, PROFILE_IMAGE_KEY, user.getProfileImage());
        TranslatorUtils.from(dbo, APPS_KEY, user.getApplicationSlugs());
        TranslatorUtils.from(dbo, MANAGES_KEY, Iterables.transform(user.getSources(), Publisher.TO_KEY));
        TranslatorUtils.from(dbo, ROLE_KEY, user.getRole().toString().toLowerCase());
        TranslatorUtils.from(dbo, PROFILE_COMPLETE_KEY, user.isProfileComplete());
        TranslatorUtils.from(dbo, PROFILE_DEACTIVATED_KEY, user.isProfileDeactivated());
        if (user.getLicenseAccepted().isPresent()) {
            TranslatorUtils.fromDateTime(dbo, LICENSE_ACCEPTED_KEY, user.getLicenseAccepted().get());
        }

        return dbo;
    }

    public User fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }

        boolean profileComplete = false;
        if (TranslatorUtils.toBoolean(dbo, PROFILE_COMPLETE_KEY) != null) {
            profileComplete = TranslatorUtils.toBoolean(dbo, PROFILE_COMPLETE_KEY);
        }

        boolean profileDeactivated = false;
        if (TranslatorUtils.toBoolean(dbo, PROFILE_DEACTIVATED_KEY) != null) {
            profileDeactivated = TranslatorUtils.toBoolean(dbo, PROFILE_DEACTIVATED_KEY);
        }

        User.Builder user = User.builder()
                .withId(TranslatorUtils.toLong(dbo, MongoConstants.ID))
                .withScreenName(TranslatorUtils.toString(dbo, SCREEN_NAME_KEY))
                .withFullName(TranslatorUtils.toString(dbo, FULL_NAME_KEY))
                .withCompany(TranslatorUtils.toString(dbo, COMPANY_KEY))
                .withEmail(TranslatorUtils.toString(dbo, EMAIL_KEY))
                .withWebsite(TranslatorUtils.toString(dbo, WEBSITE_KEY))
                .withProfileImage(TranslatorUtils.toString(dbo, PROFILE_IMAGE_KEY))
                .withApplicationSlugs(TranslatorUtils.toSet(dbo, APPS_KEY))
                .withSources(ImmutableSet.copyOf(Iterables.transform(TranslatorUtils.toSet(dbo, MANAGES_KEY),Publisher.FROM_KEY)))
                .withRole(Role.valueOf(TranslatorUtils.toString(dbo, ROLE_KEY).toUpperCase()))
                .withProfileComplete(profileComplete)
                .withLicenseAccepted(TranslatorUtils.toDateTime(dbo, LICENSE_ACCEPTED_KEY))
                .withProfileDeactivated(profileDeactivated);

        DBObject userRef = TranslatorUtils.toDBObject(dbo, USER_REF_KEY);
        if (userRef != null) {
            user.withUserRef(userTranslator.fromDBObject(userRef));
        }

        return user.build();
    }

}