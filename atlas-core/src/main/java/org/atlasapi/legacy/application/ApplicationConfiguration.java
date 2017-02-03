package org.atlasapi.legacy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.application.v3.ApplicationAccessRole;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class ApplicationConfiguration {

    /**
     * @deprecated Use {@link ApplicationConfiguration#defaultConfiguration()} instead
     */
    @Deprecated
    public static final ApplicationConfiguration DEFAULT_CONFIGURATION =
            new ApplicationConfiguration(ImmutableMap.of(), null);

    private static final ApplicationConfiguration NO_API_KEY_CONFIG = ApplicationConfiguration
            .defaultConfiguration()
            .copyWithSourceStatuses(noAPIKeySourceStatusMap());

    private final Map<Publisher, SourceStatus> sourceStatuses;
    private final Set<Publisher> enabledSources;
    private final List<Publisher> precedence;
    private final ImmutableSet<Publisher> writableSources;
    private final Boolean imagePrecedenceEnabled;
    private final Optional<List<Publisher>> contentHierarchyPrecedence;
    private final ImmutableSet<ApplicationAccessRole> accessRoles;

    private ApplicationConfiguration(
            Map<Publisher, SourceStatus> sourceStatuses,
            Set<Publisher> enabledSources,
            @Nullable List<Publisher> precedence,
            Iterable<Publisher> writable,
            @Nullable Boolean imagePrecedenceEnabled,
            Optional<List<Publisher>> contentHierarchyPrecedence,
            Set<ApplicationAccessRole> accessRoles
    ) {
        this.sourceStatuses = ImmutableMap.copyOf(sourceStatuses);
        this.enabledSources = ImmutableSet.copyOf(enabledSources);
        if(precedence == null) {
            this.precedence = null;
        } else {
            this.precedence = appendMissingPublishersTo(precedence);
        }
        this.writableSources = ImmutableSet.copyOf(writable);
        this.imagePrecedenceEnabled = imagePrecedenceEnabled;
        if (contentHierarchyPrecedence.isPresent()) {
            this.contentHierarchyPrecedence = Optional.of(
                    ImmutableList.copyOf(contentHierarchyPrecedence.get())
            );
        } else {
            this.contentHierarchyPrecedence = Optional.absent();
        }
        this.accessRoles = ImmutableSet.copyOf(
                Sets.union(accessRoles, ApplicationAccessRole.getDefaultRoles())
        );
    }

    /**
     * @deprecated Use {@link ApplicationConfiguration#builder()}
     */
    @Deprecated
    ApplicationConfiguration(
            Map<Publisher, SourceStatus> sourceStatuses,
            List<Publisher> precedence
    ) {
        this(sourceStatuses, precedence, ImmutableSet.of(), null, Optional.absent());
    }

    /**
     * @deprecated Use {@link ApplicationConfiguration#builder()}
     */
    @Deprecated
    ApplicationConfiguration(
            Map<Publisher, SourceStatus> sourceStatuses,
            List<Publisher> precedence,
            Iterable<Publisher> writableSources
    ) {
        this(
                sourceStatuses,
                enabledPublishers(sourceStatuses),
                precedence,
                writableSources,
                null,
                Optional.absent(),
                ImmutableSet.of()
        );
    }

    /**
     * @deprecated Use {@link ApplicationConfiguration#builder()}
     */
    @Deprecated
    ApplicationConfiguration(
            Map<Publisher, SourceStatus> sourceStatuses,
            List<Publisher> precedence,
            Iterable<Publisher> writableSources,
            Boolean imagePrecedenceEnabled,
            Optional<List<Publisher>> contentHierarchyPrecedence
    ) {
        this(
                sourceStatuses,
                enabledPublishers(sourceStatuses),
                precedence,
                writableSources,
                imagePrecedenceEnabled,
                contentHierarchyPrecedence,
                ImmutableSet.of()
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ApplicationConfiguration defaultConfiguration() {
        return DEFAULT_CONFIGURATION;
    }

    public static ApplicationConfiguration forNoApiKey() {
        return NO_API_KEY_CONFIG;
    }

    private static Map<Publisher, SourceStatus> noAPIKeySourceStatusMap() {
        return Maps.toMap(
                Publisher.all(),
                input -> input.enabledWithNoApiKey()
                         ? SourceStatus.AVAILABLE_ENABLED
                         : SourceStatus.UNAVAILABLE
        );
    }

    private static Set<Publisher> enabledPublishers(Map<Publisher, SourceStatus> sourceStatuses) {
        return ImmutableSet.copyOf(Maps.filterValues(
                allSourcesStatuses(sourceStatuses), SourceStatus.IS_ENABLED).keySet()
        );
    }

    private static Map<Publisher, SourceStatus> allSourcesStatuses(
            Map<Publisher, SourceStatus> configSpecificStatuses
    ) {
        ImmutableMap.Builder<Publisher, SourceStatus> statuses = ImmutableMap.builder();
        for (Publisher source : ImmutableSet.copyOf(Publisher.values())) {
            statuses.put(
                    source,
                    Optional.fromNullable(configSpecificStatuses.get(source))
                            .or(source.getDefaultSourceStatus())
            );
        }
        return statuses.build();
    }

    public boolean isEnabled(Publisher source) {
        return enabledSources.contains(source);
    }

    public Set<Publisher> getEnabledSources() {
        return enabledSources;
    }

    public SourceStatus statusOf(Publisher source) {
        return Optional.fromNullable(sourceStatuses.get(source))
                .or(source.getDefaultSourceStatus());
    }

    public Map<Publisher, SourceStatus> sourceStatuses() {
        return allSourcesStatuses(sourceStatuses);
    }

    public ApplicationConfiguration enable(Publisher source) {
        return withSource(source, statusOf(source).enable());
    }

    public ApplicationConfiguration disable(Publisher source) {
        return withSource(source, statusOf(source).disable());
    }

    public ApplicationConfiguration request(Publisher source) {
        return withSource(source, statusOf(source).request());
    }

    public ApplicationConfiguration deny(Publisher source) {
        return withSource(source, statusOf(source).deny());
    }

    public ApplicationConfiguration revoke(Publisher source) {
        return withSource(source, statusOf(source).revoke());
    }

    public ApplicationConfiguration approve(Publisher source) {
        return withSource(source, statusOf(source).approve());
    }

    public ApplicationConfiguration agreeLicense(Publisher source) {
        return withSource(source, statusOf(source).agreeLicense());
    }

    public ApplicationConfiguration reset(Publisher source) {
        return withSource(source, statusOf(source).reset(source));
    }

    public ApplicationConfiguration withSource(Publisher source, SourceStatus status) {
        HashMap<Publisher,SourceStatus> mutableSources = Maps.newHashMap(sourceStatuses);
        mutableSources.put(source, status);

        Set<Publisher> mutablePublishers = Sets.newHashSet(enabledSources);
        if (status.isEnabled()) {
            mutablePublishers.add(source);
        } else {
            mutablePublishers.remove(source);
        }

        return new ApplicationConfiguration(
                mutableSources,
                mutablePublishers,
                precedence,
                writableSources,
                imagePrecedenceEnabled,
                contentHierarchyPrecedence,
                ImmutableSet.of()
        );
    }

    public ApplicationConfiguration withSources(Map<Publisher, SourceStatus> statuses) {
        HashMap<Publisher,SourceStatus> mutableSources = Maps.newHashMap(sourceStatuses);
        mutableSources.putAll(statuses);

        return new ApplicationConfiguration(mutableSources, precedence);
    }

    public ApplicationConfiguration copyWithPrecedence(List<Publisher> publishers) {
        return new ApplicationConfiguration(sourceStatuses, ImmutableList.copyOf(publishers));
    }

    public ApplicationConfiguration copyWithContentHierarchyPrecedence(
            List<Publisher> contentHierarchyPrecedence
    ) {
        return new ApplicationConfiguration(
                sourceStatuses,
                enabledSources,
                precedence,
                writableSources,
                imagePrecedenceEnabled,
                Optional.fromNullable(contentHierarchyPrecedence),
                ImmutableSet.of()
        );
    }

    public ApplicationConfiguration copyWithImagePrecedenceEnabled(Boolean imagePrecedenceEnabled) {
        return new ApplicationConfiguration(
                sourceStatuses,
                enabledSources,
                precedence,
                writableSources,
                imagePrecedenceEnabled,
                contentHierarchyPrecedence,
                ImmutableSet.of()
        );
    }

    public ApplicationConfiguration copyWithNullPrecedence() {
        return new ApplicationConfiguration(sourceStatuses, null);
    }

    public ApplicationConfiguration copyWithWritableSources(Iterable<Publisher> writable) {
        return new ApplicationConfiguration(
                sourceStatuses,
                enabledSources,
                precedence,
                writable,
                imagePrecedenceEnabled,
                contentHierarchyPrecedence,
                ImmutableSet.of()
        );
    }

    public ApplicationConfiguration copyWithSourceStatuses(Map<Publisher, SourceStatus> statuses) {
        return new ApplicationConfiguration(
                statuses,
                precedence,
                writableSources,
                imagePrecedenceEnabled,
                contentHierarchyPrecedence
        );
    }

    public ApplicationConfiguration copyWithAccessRoles(Set<ApplicationAccessRole> accessRoles) {
        return new ApplicationConfiguration(
                sourceStatuses,
                enabledSources,
                precedence,
                writableSources,
                imagePrecedenceEnabled,
                contentHierarchyPrecedence,
                accessRoles
        );
    }

    public boolean canWrite(Publisher source) {
        return writableSources.contains(source);
    }

    public ImmutableSet<Publisher> writableSources() {
        return writableSources;
    }

    public Ordering<Publisher> publisherPrecedenceOrdering() {
        return Ordering.explicit(precedence);
    }

    public List<Publisher> precedence() {
        return precedence;
    }

    public boolean precedenceEnabled() {
        return precedence != null;
    }

    public Optional<List<Publisher>> contentHierarchyPrecedence() {
        return contentHierarchyPrecedence;
    }

    public List<Publisher> orderdPublishers() {
        if (!precedenceEnabled()) {
            return Publisher.all().asList();
        }
        return precedence;
    }

    /**
     * Temporary: these should be persisted and not hardcoded
     */
    private ImmutableList<Publisher> imagePrecedence() {
        return ImmutableList.of(Publisher.PA, Publisher.BBC, Publisher.C4);
    }

    public boolean imagePrecedenceEnabled() {
        // Inverted check to be null-safe and default to true if
        // imagePrecedenceEnabled is null
        return !Boolean.FALSE.equals(imagePrecedenceEnabled);
    }

    protected Boolean imagePrecedenceEnabledRawValue() {
        return imagePrecedenceEnabled;
    }

    public Ordering<Publisher> imagePrecedenceOrdering() {
        return publisherPrecedenceOrdering();
    }

    private List<Publisher> appendMissingPublishersTo(Iterable<Publisher> selected) {
        List<Publisher> publishers = Lists.newArrayList(selected);
        for (Publisher publisher : Publisher.values()) {
            if (!publishers.contains(publisher)) {
                publishers.add(publisher);
            }
        }
        return publishers;
    }

    private ImmutableList<Publisher> peoplePrecedence() {
        return ImmutableList.of(
                Publisher.RADIO_TIMES, Publisher.PA, Publisher.BBC, Publisher.C4, Publisher.ITV
        );
    }

    public boolean peoplePrecedenceEnabled() {
        return peoplePrecedence() != null;
    }

    public Ordering<Publisher> peoplePrecedenceOrdering() {
        return Ordering.explicit(appendMissingPublishersTo(peoplePrecedence()));
    }

    public boolean hasAccessRole(ApplicationAccessRole role) {
        return accessRoles.contains(role);
    }

    public ImmutableSet<ApplicationAccessRole> getAccessRoles() {
        return accessRoles;
    }

    public static class Builder {

        private Map<Publisher, SourceStatus> sourceStatuses;
        private Set<Publisher> enabledSources;
        private List<Publisher> precedence;
        private Iterable<Publisher> writableSources;
        private Boolean imagePrecedenceEnabled;
        private Optional<List<Publisher>> contentHierarchyPrecedence;
        private Set<ApplicationAccessRole> accessRoles = Sets.newHashSet();

        private Builder() { }

        public Builder withSourceStatuses(Map<Publisher, SourceStatus> sourceStatuses) {
            this.sourceStatuses = sourceStatuses;
            this.enabledSources = enabledPublishers(sourceStatuses);
            return this;
        }

        public Builder withPrecedence(@Nullable List<Publisher> precedence) {
            this.precedence = precedence;
            return this;
        }

        public Builder withWritableSources(Iterable<Publisher> writableSources) {
            this.writableSources = writableSources;
            return this;
        }

        public Builder withImagePrecedenceEnabled(@Nullable Boolean imagePrecedenceEnabled) {
            this.imagePrecedenceEnabled = imagePrecedenceEnabled;
            return this;
        }

        public Builder withContentHierarchyPrecedence(
                Optional<List<Publisher>> contentHierarchyPrecedence
        ) {
            this.contentHierarchyPrecedence = contentHierarchyPrecedence;
            return this;
        }

        public Builder withAccessRoles(Set<ApplicationAccessRole> accessRoles) {
            this.accessRoles = accessRoles;
            return this;
        }

        public ApplicationConfiguration build() {
            return new ApplicationConfiguration(
                    sourceStatuses,
                    enabledSources,
                    precedence,
                    writableSources,
                    imagePrecedenceEnabled,
                    contentHierarchyPrecedence,
                    accessRoles
            );
        }
    }
}