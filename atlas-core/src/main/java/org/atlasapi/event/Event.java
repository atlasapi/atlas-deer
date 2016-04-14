package org.atlasapi.event;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.organisation.OrganisationRef;
import org.atlasapi.topic.Topic;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;

public class Event extends Identified implements Sourced {

    private final String title;
    private final Publisher source;
    private final Topic venue;
    private final DateTime startTime;
    private final DateTime endTime;
    private final ImmutableList<Person> participants;
    private final ImmutableList<OrganisationRef> organisations;
    private final ImmutableList<Topic> eventGroups;
    private final ImmutableList<ItemRef> content;

    private Event(Builder<?, ?> builder) {
        super(builder);
        this.title = checkNotNull(builder.title);
        this.source = checkNotNull(builder.source);
        this.venue = builder.venue;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.participants = ImmutableList.copyOf(builder.participants);
        this.organisations = ImmutableList.copyOf(builder.organisations);
        this.eventGroups = ImmutableList.copyOf(builder.eventGroups);
        this.content = ImmutableList.copyOf(builder.content);
    }

    public String getTitle() {
        return title;
    }

    @Override
    public Publisher getSource() {
        return source;
    }

    public Topic getVenue() {
        return venue;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public ImmutableList<Person> getParticipants() {
        return participants;
    }

    public ImmutableList<OrganisationRef> getOrganisations() {
        return organisations;
    }

    public ImmutableList<Topic> getEventGroups() {
        return eventGroups;
    }

    public ImmutableList<ItemRef> getContent() {
        return content;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(Event.class)
                .add("title", title)
                .add("source", source)
                .add("venue", venue)
                .add("startTime", startTime)
                .add("endTime", endTime)
                .add("participants", participants)
                .add("organisations", organisations)
                .add("eventGroups", eventGroups)
                .add("content", content)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Event) {
            Event target = (Event) obj;
            return getId() != null ? Objects.equal(getId(), target.getId())
                                   : Objects.equal(getCanonicalUri(), target.getCanonicalUri());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getId() != null ? getId().hashCode() : getCanonicalUri().hashCode();
    }

    public static EventBuilder builder() {
        return new EventBuilder();
    }

    public abstract static class Builder<T extends Event, B extends Builder<T, B>>
            extends Identified.Builder<T, B> {

        private String title;
        private Publisher source;
        private Topic venue;
        private DateTime startTime;
        private DateTime endTime;
        private ImmutableList<Person> participants = ImmutableList.of();
        private ImmutableList<OrganisationRef> organisations = ImmutableList.of();
        private ImmutableList<Topic> eventGroups = ImmutableList.of();
        private ImmutableList<ItemRef> content = ImmutableList.of();

        protected Builder() {
        }

        public B withTitle(String title) {
            this.title = title;
            return self();
        }

        public B withSource(Publisher publisher) {
            this.source = publisher;
            return self();
        }

        public B withVenue(Topic venue) {
            this.venue = venue;
            return self();
        }

        public B withStartTime(DateTime startTime) {
            this.startTime = startTime;
            return self();
        }

        public B withEndTime(DateTime endTime) {
            this.endTime = endTime;
            return self();
        }

        public B withParticipants(Iterable<Person> participants) {
            this.participants = ImmutableList.copyOf(participants);
            return self();
        }

        public B withOrganisations(Iterable<OrganisationRef> organisations) {
            this.organisations = ImmutableList.copyOf(organisations);
            return self();
        }

        public B withEventGroups(Iterable<Topic> eventGroups) {
            this.eventGroups = ImmutableList.copyOf(eventGroups);
            return self();
        }

        public B withContent(Iterable<ItemRef> content) {
            this.content = ImmutableList.copyOf(content);
            return self();
        }
    }

    public static class EventBuilder extends Builder<Event, EventBuilder> {

        private EventBuilder() {
        }

        @Override
        public Event build() {
            return new Event(this);
        }

        @Override
        protected EventBuilder self() {
            return this;
        }
    }
}
