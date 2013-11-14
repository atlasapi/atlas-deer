package org.atlasapi.model.translators;

import java.util.Set;

import org.atlasapi.application.Application;
import org.atlasapi.application.LegacyApplicationStore;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


public class UserModelTranslator {
    private final LegacyApplicationStore store;
    
    public UserModelTranslator(LegacyApplicationStore store) {
        this.store = store;
    }

    public User transform3to4(org.atlasapi.application.users.v3.User input) {
        return User.builder()
                .withId(Id.valueOf(input.getId().longValue()))
                .withUserRef(input.getUserRef())
                .withScreenName(input.getScreenName())
                .withFullName(input.getFullName())
                .withCompany(input.getCompany())
                .withEmail(input.getEmail())
                .withWebsite(input.getWebsite())
                .withProfileImage(input.getProfileImage())
                .withApplicationIds(transformApplicationSlugs(input.getApplicationSlugs()))
                .withSources(input.getSources())
                .withRole(input.getRole())
                .withProfileComplete(input.isProfileComplete())
                .build();
    }
    
    public Set<Id> transformApplicationSlugs(Set<String> input) {
        return Sets.newHashSet(store.applicationIdsForSlugs(input));
    }
    
    public org.atlasapi.application.users.v3.User transform4to3(User input) {
        return org.atlasapi.application.users.v3.User.builder()
                .withId(input.getId().longValue())
                .withUserRef(input.getUserRef())
                .withScreenName(input.getScreenName())
                .withFullName(input.getFullName())
                .withCompany(input.getCompany())
                .withEmail(input.getEmail())
                .withWebsite(input.getWebsite())
                .withProfileImage(input.getProfileImage())
                .withRole(input.getRole())
                .withApplicationSlugs(transformApplicationIds(input.getApplicationIds()))
                .withSources(input.getSources())
                .withProfileComplete(input.isProfileComplete())
                .build();
    }
    
    public Set<String> transformApplicationIds(Set<Id> input) {
        Iterable<Application> applications = store.applicationsFor(input);
        
        return Sets.newHashSet(Iterables.transform(applications, new Function<Application, String>() {

            @Override
            public String apply(Application input) {
               return input.getSlug();
            }}));
    }
}
