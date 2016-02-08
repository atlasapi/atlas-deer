package org.atlasapi.application;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.media.entity.Publisher;

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
    private final Optional<DateTime> approvedAt; // Older source request records will not have this field

    private SourceRequest(Id id, Id appId, Publisher source, UsageType usageType,
            String email, String appUrl, String reason, boolean licenseAccepted,
            DateTime requestedAt, boolean approved, Optional<DateTime> approvedAt) {
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

    public Id getId() {
        return this.id;
    }

    public Id getAppId() {
        return appId;
    }

    public Publisher getSource() {
        return source;
    }

    public UsageType getUsageType() {
        return usageType;
    }

    public String getEmail() {
        return email;
    }

    public String getAppUrl() {
        return appUrl;
    }

    public String getReason() {
        return reason;
    }

    public boolean isLicenseAccepted() {
        return licenseAccepted;
    }

    public DateTime getRequestedAt() {
        return requestedAt;
    }

    public boolean isApproved() {
        return approved;
    }

    public Optional<DateTime> getApprovedAt() {
        return approvedAt;
    }

    public static Builder builder() {
        return new Builder();
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

        public Builder withId(Id id) {
            this.id = id;
            return this;
        }

        public Builder withAppId(Id appId) {
            this.appId = appId;
            return this;
        }

        public Builder withSource(Publisher source) {
            this.source = source;
            return this;
        }

        public Builder withUsageType(UsageType usageType) {
            this.usageType = usageType;
            return this;
        }

        public Builder withEmail(String email) {
            this.email = email;
            return this;
        }

        public Builder withAppUrl(String appUrl) {
            this.appUrl = appUrl;
            return this;
        }

        public Builder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public Builder withLicenseAccepted(boolean licenseAccepted) {
            this.licenseAccepted = licenseAccepted;
            return this;
        }

        public Builder withRequestedAt(DateTime requestedAt) {
            this.requestedAt = requestedAt;
            return this;
        }

        public Builder withApproved(boolean approved) {
            this.approved = approved;
            return this;
        }

        public Builder withApprovedAt(DateTime approvedAt) {
            this.approvedAt = Optional.fromNullable(approvedAt);
            return this;
        }

        public Builder withApprovedAt(Optional<DateTime> approvedAt) {
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