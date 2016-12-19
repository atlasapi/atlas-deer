package org.atlasapi.model.translators;

import java.util.Set;

import org.atlasapi.application.Application;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class UserModelTranslator implements Function<org.atlasapi.application.users.v3.User, User> {

    public UserModelTranslator() {
    }

    public User apply(org.atlasapi.application.users.v3.User input) {
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
//        return Sets.newHashSet(store.applicationIdsForSlugs(input)); //TODO: this
        return Sets.newHashSet();
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
                .withRole(org.atlasapi.application.users.v3.Role.valueOf(input.getRole().name()))
                .withApplicationSlugs(transformApplicationIds(input.getApplicationIds()))
                .withSources(input.getSources())
                .withProfileComplete(input.isProfileComplete())
                .withLicenseAccepted(input.getLicenseAccepted().orNull())
                .withProfileDeactivated(input.isProfileDeactivated())
                .build();
    }

    public Set<String> transformApplicationIds(Set<Id> input) {
        ListenableFuture<Resolved<Application>> resolved = null; // store.resolveIds(input); // TODO: this
        return ImmutableSet.copyOf(Futures.getUnchecked(resolved)
                .getResources()
                .transform(Application::getSlug)
        );
    }
}
