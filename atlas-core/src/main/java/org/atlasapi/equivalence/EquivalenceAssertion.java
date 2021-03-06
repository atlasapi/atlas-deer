package org.atlasapi.equivalence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalenceAssertion {

    private final ResourceRef subject;
    private final ImmutableSet<ResourceRef> assertedAdjacents;
    private final ImmutableSet<Publisher> sources;

    // Visible for Jackson
    @SuppressWarnings("WeakerAccess")
    @JsonCreator
    EquivalenceAssertion(
            @JsonProperty("subject") ResourceRef subject,
            @Nullable @JsonProperty("assertedAdjacents") Iterable<ResourceRef> assertedAdjacents,
            @Nullable @JsonProperty("sources") Iterable<Publisher> sources
    ) {
        this.subject = checkNotNull(subject);
        this.assertedAdjacents = assertedAdjacents == null
                                 ? ImmutableSet.of()
                                 : ImmutableSet.copyOf(assertedAdjacents);
        this.sources = sources == null
                       ? ImmutableSet.of()
                       : ImmutableSet.copyOf(sources);
    }

    public static EquivalenceAssertion create(
            ResourceRef subject,
            Iterable<ResourceRef> assertedAdjacents,
            Iterable<Publisher> sources
    ) {
        return new EquivalenceAssertion(
                subject, assertedAdjacents, sources
        );
    }

    @JsonProperty("subject")
    public ResourceRef getSubject() {
        return subject;
    }

    @JsonProperty("assertedAdjacents")
    public ImmutableSet<ResourceRef> getAssertedAdjacents() {
        return assertedAdjacents;
    }

    @JsonProperty("sources")
    public ImmutableSet<Publisher> getSources() {
        return sources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EquivalenceAssertion that = (EquivalenceAssertion) o;
        return Objects.equals(subject, that.subject) &&
                Objects.equals(assertedAdjacents, that.assertedAdjacents) &&
                Objects.equals(sources, that.sources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, assertedAdjacents, sources);
    }

    @Override
    public String toString() {
        return com.google.common.base.MoreObjects.toStringHelper(this)
                .add("subject", subject)
                .add("assertedAdjacents", assertedAdjacents)
                .add("sources", sources)
                .toString();
    }
}
