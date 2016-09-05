package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalenceAssertionMessage extends AbstractMessage {

    public static final class Builder {

        private final String messageId;
        private final Timestamp timestamp;
        private final ResourceRef subject;

        private ImmutableSet<ResourceRef> assertedAdjacents = ImmutableSet.of();
        private ImmutableSet<Publisher> publishers = ImmutableSet.of();

        public Builder(String messageId, Timestamp timestamp, ResourceRef subject) {
            this.messageId = checkNotNull(messageId);
            this.timestamp = checkNotNull(timestamp);
            this.subject = checkNotNull(subject);
        }

        public Builder withAssertedAdjacents(Iterable<ResourceRef> assertedAdjacents) {
            this.assertedAdjacents = ImmutableSet.copyOf(assertedAdjacents);
            return this;
        }

        public Builder withPublishers(Iterable<Publisher> publishers) {
            this.publishers = ImmutableSet.copyOf(publishers);
            return this;
        }

        public EquivalenceAssertionMessage build() {
            return new EquivalenceAssertionMessage(
                    messageId,
                    timestamp,
                    subject,
                    assertedAdjacents,
                    publishers
            );
        }

    }

    private final ResourceRef subject;
    private final ImmutableSet<ResourceRef> assertedAdjacents;
    private final ImmutableSet<Publisher> publishers;

    @JsonCreator
    public EquivalenceAssertionMessage(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("subject") ResourceRef subject,
            @JsonProperty("assertedAdjacents") Set<? extends ResourceRef> assertedAdjacents,
            @JsonProperty("publishers") Set<Publisher> publishers
    ) {
        super(messageId, timestamp);
        this.subject = subject;
        this.assertedAdjacents = ImmutableSet.copyOf(assertedAdjacents);
        this.publishers = ImmutableSet.copyOf(publishers);
    }

    public ResourceRef getSubject() {
        return subject;
    }

    public ImmutableSet<ResourceRef> getAssertedAdjacents() {
        return assertedAdjacents;
    }

    public ImmutableSet<Publisher> getPublishers() {
        return publishers;
    }

}
