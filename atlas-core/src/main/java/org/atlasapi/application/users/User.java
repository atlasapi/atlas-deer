package org.atlasapi.application.users;

import java.util.Set;

import org.atlasapi.application.Application;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

public class User implements Identifiable {

    private final Id id;
    private final UserRef userRef;
    private final String screenName;
    private final String fullName;
    private final String company;
    private final String email;
    private final String website;
    private final String profileImage;
    private final Role role;
    private final boolean profileComplete;
    private final Optional<DateTime> licenseAccepted;
    private final boolean profileDeactivated;

    private final Set<Id> applicationIds;
    private final Set<Publisher> sources;

    private User(Id id, UserRef userRef, String screenName, String fullName,
            String company, String email, String website, String profileImage, Role role,
            Set<Id> applicationIds, Set<Publisher> publishers, boolean profileComplete,
            Optional<DateTime> licenseAccepted, boolean profileDeactivated) {
        this.id = id;
        this.userRef = userRef;
        this.screenName = screenName;
        this.fullName = fullName;
        this.company = company;
        this.email = email;
        this.website = website;
        this.profileImage = profileImage;
        this.role = role;
        this.applicationIds = ImmutableSet.copyOf(applicationIds);
        this.sources = ImmutableSet.copyOf(publishers);
        this.profileComplete = profileComplete;
        this.licenseAccepted = licenseAccepted;
        this.profileDeactivated = profileDeactivated;
    }

    public UserRef getUserRef() {
        return this.userRef;
    }

    public String getScreenName() {
        return screenName;
    }

    public String getFullName() {
        return fullName;
    }

    public String getCompany() {
        return company;
    }

    public String getEmail() {
        return email;
    }

    public String getWebsite() {
        return website;
    }

    public String getProfileImage() {
        return profileImage;
    }

    public Set<Id> getApplicationIds() {
        return applicationIds;
    }

    public Set<Publisher> getSources() {
        return sources;
    }

    public Role getRole() {
        return this.role;
    }

    public boolean isProfileComplete() {
        return profileComplete;
    }

    public Optional<DateTime> getLicenseAccepted() {
        return licenseAccepted;
    }

    public boolean manages(Application application) {
        return manages(application.getId());
    }

    public boolean manages(Id applicationId) {
        return applicationIds.contains(applicationIds);
    }

    public boolean manages(Optional<Publisher> possibleSource) {
        return possibleSource.isPresent() && sources.contains(possibleSource.get());
    }

    public Id getId() {
        return this.id;
    }

    public boolean is(Role role) {
        return this.role == role;
    }

    public boolean isProfileDeactivated() {
        return profileDeactivated;
    }

    public User copyWithAdditionalApplication(Application application) {
        Set<Id> applicationIds = ImmutableSet.<Id>builder().add(application.getId())
                .addAll(this.getApplicationIds())
                .build();
        return this.copy().withApplicationIds(applicationIds).build();
    }

    public Builder copy() {
        return new Builder()
                .withId(this.getId())
                .withUserRef(this.getUserRef())
                .withScreenName(this.getScreenName())
                .withFullName(this.getFullName())
                .withCompany(this.getCompany())
                .withEmail(this.getEmail())
                .withWebsite(this.getWebsite())
                .withProfileImage(this.getProfileImage())
                .withApplicationIds(this.getApplicationIds())
                .withSources(this.getSources())
                .withRole(this.getRole())
                .withProfileComplete(this.isProfileComplete())
                .withLicenseAccepted(this.getLicenseAccepted().orNull())
                .withProfileDeactivated(this.profileDeactivated);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Id id;
        private UserRef userRef;
        private String screenName;
        private String fullName;
        private String company;
        private String email;
        private String website;
        private String profileImage;
        private Role role = Role.REGULAR;
        private Set<Id> applicationIds = ImmutableSet.of();
        private Set<Publisher> sources = ImmutableSet.of();
        private boolean profileComplete = false;
        private Optional<DateTime> licenseAccepted = Optional.absent();
        private boolean profileDeactivated = false;

        public Builder withId(Id id) {
            this.id = id;
            return this;
        }

        public Builder withUserRef(UserRef userRef) {
            this.userRef = userRef;
            return this;
        }

        public Builder withScreenName(String screenName) {
            this.screenName = screenName;
            return this;
        }

        public Builder withFullName(String fullName) {
            this.fullName = fullName;
            return this;
        }

        public Builder withCompany(String company) {
            this.company = company;
            return this;
        }

        public Builder withEmail(String email) {
            this.email = email;
            return this;
        }

        public Builder withWebsite(String website) {
            this.website = website;
            return this;
        }

        public Builder withProfileImage(String profileImage) {
            this.profileImage = profileImage;
            return this;
        }

        public Builder withRole(Role role) {
            this.role = role;
            return this;
        }

        public Builder withApplicationIds(Set<Id> applicationIds) {
            this.applicationIds = applicationIds;
            return this;
        }

        public Builder withSources(Set<Publisher> sources) {
            this.sources = sources;
            return this;
        }

        public Builder withProfileComplete(boolean profileComplete) {
            this.profileComplete = profileComplete;
            return this;
        }

        public Builder withLicenseAccepted(DateTime licenseAccepted) {
            this.licenseAccepted = Optional.fromNullable(licenseAccepted);
            return this;
        }

        public Builder withProfileDeactivated(boolean profileDeactivated) {
            this.profileDeactivated = profileDeactivated;
            return this;
        }

        public User build() {
            Preconditions.checkNotNull(id);
            return new User(id, userRef, screenName, fullName,
                    company, email, website, profileImage, role,
                    applicationIds, sources, profileComplete, licenseAccepted, profileDeactivated
            );
        }
    }
}
