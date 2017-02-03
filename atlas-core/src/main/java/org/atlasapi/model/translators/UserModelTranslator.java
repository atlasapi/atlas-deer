package org.atlasapi.model.translators;

import java.util.Set;

import org.atlasapi.application.Application;
import org.atlasapi.application.LegacyApplicationStore;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class UserModelTranslator implements Function<org.atlasapi.legacy.user.User, User> {

    private final LegacyApplicationStore store;

    public UserModelTranslator(LegacyApplicationStore store) {
        this.store = store;
    }

    public User apply(org.atlasapi.legacy.user.User input) {
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
                .withRole(Role.valueOf(input.getRole().name()))
                .withProfileComplete(input.isProfileComplete())
                .withLicenseAccepted(input.getLicenseAccepted().orNull())
                .withProfileDeactivated(input.isProfileDeactivated())
                .build();
    }

    public Set<Id> transformApplicationSlugs(Set<String> input) {
        return Sets.newHashSet(store.applicationIdsForSlugs(input));
    }

    public org.atlasapi.legacy.user.User transform4to3(User input) {
        return org.atlasapi.legacy.user.User.builder()
                .withId(input.getId().longValue())
                .withUserRef(input.getUserRef())
                .withScreenName(input.getScreenName())
                .withFullName(input.getFullName())
                .withCompany(input.getCompany())
                .withEmail(input.getEmail())
                .withWebsite(input.getWebsite())
                .withProfileImage(input.getProfileImage())
                .withRole(org.atlasapi.legacy.user.Role.valueOf(input.getRole().name()))
                .withApplicationSlugs(transformApplicationIds(input.getApplicationIds()))
                .withSources(input.getSources())
                .withProfileComplete(input.isProfileComplete())
                .withLicenseAccepted(input.getLicenseAccepted().orNull())
                .withProfileDeactivated(input.isProfileDeactivated())
                .build();
    }

    public Set<String> transformApplicationIds(Set<Id> input) {
        ListenableFuture<Resolved<Application>> resolved = store.resolveIds(input);
        return ImmutableSet.copyOf(Futures.getUnchecked(resolved)
                .getResources().transform(new Function<Application, String>() {

                                              @Override
                                              public String apply(Application input) {
                                                  return input.getSlug();
                                              }
                                          }
                ));
    }
}
