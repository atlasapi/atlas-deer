package org.atlasapi.application;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import org.joda.time.DateTime;

public class SourceRequest implements Identifiable {

    private final Id id;
    private final Id appId;
    private final Publisher source;
    private final UsageType usageType;
    private final String email;
    private final String appUrl;
    private final String reason;
    private final boolean licenseAccepted;
    private final DateTime requestedAt;
    private final boolean approved;

    // Older source request records will not have this field
    private final Optional<DateTime> approvedAt;

    private SourceRequest(
            @Nullable Id id,
            @Nullable Id appId,
            @Nullable Publisher source,
            @Nullable UsageType usageType,
            @Nullable String email,
            @Nullable String appUrl,
            @Nullable String reason,
            boolean licenseAccepted,
            @Nullable DateTime requestedAt,
            boolean approved,
            @Nullable Optional<DateTime> approvedAt
    ) {
        this.id = id;
        this.appId = appId;
        this.source = source;
        this.usageType = usageType;
        this.email = email;
        this.appUrl = appUrl;
        this.reason = reason;
        this.licenseAccepted = licenseAccepted;
        this.requestedAt = requestedAt;
        this.approved = approved;
        this.approvedAt = approvedAt;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Nullable
    public Id getId() {
        return this.id;
    }

    @Nullable
    public Id getAppId() {
        return appId;
    }

    @Nullable
    public Publisher getSource() {
        return source;
    }

    @Nullable
    public UsageType getUsageType() {
        return usageType;
    }

    @Nullable
    public String getEmail() {
        return email;
    }

    @Nullable
    public String getAppUrl() {
        return appUrl;
    }

    @Nullable
    public String getReason() {
        return reason;
    }

    public boolean isLicenseAccepted() {
        return licenseAccepted;
    }

    @Nullable
    public DateTime getRequestedAt() {
        return requestedAt;
    }

    public boolean isApproved() {
        return approved;
    }

    @Nullable
    public Optional<DateTime> getApprovedAt() {
        return approvedAt;
    }

    public Builder copy() {
        return new Builder()
                .withId(id)
                .withAppId(appId)
                .withSource(source)
                .withUsageType(usageType)
                .withEmail(email)
                .withAppUrl(appUrl)
                .withReason(reason)
                .withRequestedAt(requestedAt)
                .withApproved(approved)
                .withApprovedAt(approvedAt);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("appId", appId)
                .add("source", source)
                .add("usageType", usageType)
                .add("email", email)
                .add("appUrl", appUrl)
                .add("reason", reason)
                .add("licenseAccepted", licenseAccepted)
                .add("requestedAt", requestedAt)
                .add("approved", approved)
                .add("approvedAt", approvedAt)
                .toString();
    }

    public static class Builder {

        private Id id;
        private Id appId;
        private Publisher source;
        private UsageType usageType;
        private String email;
        private String appUrl;
        private String reason;
        private boolean licenseAccepted = false;
        private DateTime requestedAt;
        private boolean approved = false;
        private Optional<DateTime> approvedAt = Optional.absent();

        public Builder() {
        }

        public Builder withId(@Nullable Id id) {
            this.id = id;
            return this;
        }

        public Builder withAppId(@Nullable Id appId) {
            this.appId = appId;
            return this;
        }

        public Builder withSource(@Nullable Publisher source) {
            this.source = source;
            return this;
        }

        public Builder withUsageType(@Nullable UsageType usageType) {
            this.usageType = usageType;
            return this;
        }

        public Builder withEmail(@Nullable String email) {
            this.email = email;
            return this;
        }

        public Builder withAppUrl(@Nullable String appUrl) {
            this.appUrl = appUrl;
            return this;
        }

        public Builder withReason(@Nullable String reason) {
            this.reason = reason;
            return this;
        }

        public Builder withLicenseAccepted(boolean licenseAccepted) {
            this.licenseAccepted = licenseAccepted;
            return this;
        }

        public Builder withRequestedAt(@Nullable DateTime requestedAt) {
            this.requestedAt = requestedAt;
            return this;
        }

        public Builder withApproved(boolean approved) {
            this.approved = approved;
            return this;
        }

        public Builder withApprovedAt(@Nullable DateTime approvedAt) {
            this.approvedAt = Optional.fromNullable(approvedAt);
            return this;
        }

        public Builder withApprovedAt(@Nullable Optional<DateTime> approvedAt) {
            this.approvedAt = approvedAt;
            return this;
        }

        public SourceRequest build() {
            // Retain approved flag for backwards compatibility
            this.approved = approvedAt.isPresent();
            return new SourceRequest(id, appId, source, usageType,
                    email, appUrl, reason, licenseAccepted, requestedAt, approved, approvedAt
            );
        }
    }
}
