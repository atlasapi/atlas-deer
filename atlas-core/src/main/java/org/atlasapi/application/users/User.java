package org.atlasapi.application.users;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.application.Application;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;

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

    private User(
            Id id,
            @Nullable UserRef userRef,
            @Nullable String screenName,
            @Nullable String fullName,
            @Nullable String company,
            @Nullable String email,
            @Nullable String website,
            @Nullable String profileImage,
            @Nullable Role role,
            Set<Id> applicationIds,
            Set<Publisher> publishers,
            boolean profileComplete,
            Optional<DateTime> licenseAccepted,
            boolean profileDeactivated
    ) {
        this.id = checkNotNull(id);
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
        this.licenseAccepted = checkNotNull(licenseAccepted);
        this.profileDeactivated = profileDeactivated;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Nullable
    public UserRef getUserRef() {
        return this.userRef;
    }

    @Nullable
    public String getScreenName() {
        return screenName;
    }

    @Nullable
    public String getFullName() {
        return fullName;
    }

    @Nullable
    public String getCompany() {
        return company;
    }

    @Nullable
    public String getEmail() {
        return email;
    }

    @Nullable
    public String getWebsite() {
        return website;
    }

    @Nullable
    public String getProfileImage() {
        return profileImage;
    }

    public Set<Id> getApplicationIds() {
        return applicationIds;
    }

    public Set<Publisher> getSources() {
        return sources;
    }

    @Nullable
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

    public boolean manages(@Nullable Id applicationId) {
        return applicationIds.contains(applicationId);
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

        public Builder withUserRef(@Nullable UserRef userRef) {
            this.userRef = userRef;
            return this;
        }

        public Builder withScreenName(@Nullable String screenName) {
            this.screenName = screenName;
            return this;
        }

        public Builder withFullName(@Nullable String fullName) {
            this.fullName = fullName;
            return this;
        }

        public Builder withCompany(@Nullable String company) {
            this.company = company;
            return this;
        }

        public Builder withEmail(@Nullable String email) {
            this.email = email;
            return this;
        }

        public Builder withWebsite(@Nullable String website) {
            this.website = website;
            return this;
        }

        public Builder withProfileImage(@Nullable String profileImage) {
            this.profileImage = profileImage;
            return this;
        }

        public Builder withRole(@Nullable Role role) {
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

        public Builder withLicenseAccepted(@Nullable DateTime licenseAccepted) {
            this.licenseAccepted = Optional.fromNullable(licenseAccepted);
            return this;
        }

        public Builder withProfileDeactivated(boolean profileDeactivated) {
            this.profileDeactivated = profileDeactivated;
            return this;
        }

        public User build() {
            return new User(id, userRef, screenName, fullName,
                    company, email, website, profileImage, role,
                    applicationIds, sources, profileComplete, licenseAccepted, profileDeactivated
            );
        }
    }
}
