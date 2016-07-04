package org.atlasapi.application;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;

public class Application implements Identifiable, Sourced {

    private final Id id;
    private final String slug; // Kept to enable creation of compatible entries for 3.0
    private final String title;
    private final String description;
    private final DateTime created;
    private final ApplicationCredentials credentials;
    private final ApplicationSources sources;
    private final boolean revoked;

    private Application(
            @Nullable Id id,
            @Nullable String slug,
            @Nullable String title,
            @Nullable String description,
            @Nullable DateTime created,
            @Nullable ApplicationCredentials credentials,
            @Nullable ApplicationSources sources,
            boolean revoked
    ) {
        this.id = id;
        this.slug = slug;
        this.title = title;
        this.description = description;
        this.created = created;
        this.credentials = credentials;
        this.sources = sources;
        this.revoked = revoked;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Nullable
    public Id getId() {
        return id;
    }

    /**
     * Returns an Atlas 3.0 compatible identifier for applications
     */
    @Deprecated
    @Nullable
    public String getSlug() {
        return slug;
    }

    @Nullable
    public String getTitle() {
        return title;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    @Nullable
    public DateTime getCreated() {
        return created;
    }

    @Nullable
    public ApplicationCredentials getCredentials() {
        return credentials;
    }

    @Nullable
    public ApplicationSources getSources() {
        return sources;
    }

    @Override
    public Publisher getSource() {
        return Publisher.METABROADCAST;
    }

    public boolean isRevoked() {
        return revoked;
    }

    public Application copyWithPrecedenceDisabled() {
        ApplicationSources modifiedSources = this.getSources()
                .copy()
                .withPrecedence(false)
                .build();
        return this.copy().withSources(modifiedSources).build();
    }

    public Application copyWithAddedWritingSource(Publisher source) {
        List<Publisher> writes = Lists.newArrayList(this.getSources().getWrites());
        if (!writes.contains(source)) {
            writes.add(source);
        }
        ApplicationSources modifiedSources = this
                .getSources().copy().withWritableSources(writes).build();
        return this.copy().withSources(modifiedSources).build();
    }

    public Application copyWithRemovedWritingSource(Publisher source) {
        List<Publisher> writes = Lists.newArrayList(this.getSources().getWrites());
        writes.remove(source);
        ApplicationSources modifiedSources = this
                .getSources().copy().withWritableSources(writes).build();
        return this.copy().withSources(modifiedSources).build();
    }

    public Application copyWithSources(ApplicationSources sources) {
        return this.copy().withSources(sources).build();
    }

    public Application copyWithReadSourceState(Publisher source, SourceState sourceState) {
        SourceStatus status = findSourceStatusFor(source, this.getSources().getReads());
        SourceStatus newStatus = status.copyWithState(sourceState);
        return copyWithStatusForSource(source, newStatus);
    }

    public Application copyWithSourceEnabled(Publisher source) {
        SourceStatus status = findSourceStatusFor(source, this.getSources().getReads());
        status = status.enable();
        return copyWithStatusForSource(source, status);
    }

    public Application copyWithSourceDisabled(Publisher source) {
        SourceStatus status = findSourceStatusFor(source, this.getSources().getReads());
        status = status.disable();
        return copyWithStatusForSource(source, status);
    }

    private Application copyWithStatusForSource(Publisher source,
            SourceStatus status) {
        List<SourceReadEntry> modifiedReads = changeReadsPreservingOrder(
                this.getSources().getReads(), source, status
        );
        ApplicationSources modifiedSources = this.getSources().copy()
                .withReadableSources(modifiedReads).build();
        return this.copy().withSources(modifiedSources).build();
    }

    private SourceStatus findSourceStatusFor(Publisher source, List<SourceReadEntry> reads) {
        for (SourceReadEntry status : reads) {
            if (status.getPublisher().equals(source)) {
                return status.getSourceStatus();
            }
        }
        return SourceStatus.fromV3SourceStatus(source.getDefaultSourceStatus());
    }

    private List<SourceReadEntry> changeReadsPreservingOrder(
            List<SourceReadEntry> original,
            Publisher sourceToChange,
            SourceStatus newSourceStatus) {
        ImmutableList.Builder<SourceReadEntry> builder = ImmutableList.builder();
        for (SourceReadEntry source : original) {
            if (source.getPublisher().equals(sourceToChange)) {
                builder.add(new SourceReadEntry(source.getPublisher(), newSourceStatus));
            } else {
                builder.add(source);
            }
        }
        return builder.build();
    }

    public Application copyWithReadSourceOrder(List<Publisher> ordering) {
        Map<Publisher, SourceReadEntry> sourceMap = getSourceReadsAsKeyedMap();
        List<Publisher> seen = Lists.newArrayList();
        List<SourceReadEntry> readsWithNewOrder = Lists.newArrayList();
        for (Publisher source : ordering) {
            readsWithNewOrder.add(sourceMap.get(source));
            seen.add(source);
        }
        // add sources omitted from ordering
        for (Publisher source : sourceMap.keySet()) {
            if (!seen.contains(source)) {
                readsWithNewOrder.add(sourceMap.get(source));
            }
        }
        ApplicationSources modifiedSources = this.getSources()
                .copy()
                .withPrecedence(true)
                .withReadableSources(readsWithNewOrder)
                .build();

        return this.copy().withSources(modifiedSources).build();
    }

    private Map<Publisher, SourceReadEntry> getSourceReadsAsKeyedMap() {
        ImmutableMap.Builder<Publisher, SourceReadEntry> sourceMap = ImmutableMap.builder();
        for (SourceReadEntry read : this.getSources().getReads()) {
            sourceMap.put(read.getPublisher(), read);
        }
        return sourceMap.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Application) {
            Application other = (Application) obj;
            return this.getId().equals(other.getId())
                    && this.getSlug().equals(other.getSlug())
                    && Objects.equal(this.getTitle(), other.getTitle())
                    && Objects.equal(this.getDescription(), other.getDescription())
                    && this.getCreated().equals(other.getCreated())
                    && this.getCredentials().equals(other.getCredentials())
                    && this.getSources().equals(other.getSources())
                    && this.isRevoked() == other.isRevoked();
        }
        return false;
    }

    public Builder copy() {
        return builder()
                .withId(this.getId())
                .withSlug(this.getSlug())
                .withTitle(this.getTitle())
                .withDescription(this.getDescription())
                .withCreated(this.getCreated())
                .withCredentials(this.getCredentials())
                .withSources(this.getSources())
                .withRevoked(this.isRevoked());
    }

    public static class Builder {

        private Id id;
        private String slug;
        private String title;
        private String description;
        private DateTime created;
        private ApplicationCredentials credentials;
        private ApplicationSources sources;
        private boolean revoked = true;

        public Builder() {
            this.sources = ApplicationSources.defaults();
        }

        public Builder withId(@Nullable Id id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the Atlas 3.0 compatible identifier for the application
         */
        @Deprecated
        public Builder withSlug(@Nullable String slug) {
            this.slug = slug;
            return this;
        }

        public Builder withTitle(@Nullable String title) {
            this.title = title;
            return this;
        }

        public Builder withDescription(@Nullable String description) {
            this.description = description;
            return this;
        }

        public Builder withCreated(@Nullable DateTime created) {
            this.created = created;
            return this;
        }

        public Builder withCredentials(@Nullable ApplicationCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        public Builder withSources(@Nullable ApplicationSources sources) {
            this.sources = sources;
            return this;
        }

        public Builder withRevoked(boolean revoked) {
            this.revoked = revoked;
            return this;
        }

        public Application build() {
            return new Application(
                    id,
                    slug,
                    title,
                    description,
                    created,
                    credentials,
                    sources,
                    revoked
            );
        }
    }
}
