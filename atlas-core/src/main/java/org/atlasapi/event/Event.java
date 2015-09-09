package org.atlasapi.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;
import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class Event extends Identified implements Sourced {

    private final String title;
    private final Publisher source;
    private final Topic venue;
    private final DateTime startTime;
    private final DateTime endTime;
    private final ImmutableList<Person> participants;
    private final ImmutableList<Organisation> organisations;
    private final ImmutableList<Topic> eventGroups;
    private final ImmutableList<ItemRef> content;

    private Event(Builder<?> builder) {
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

    public String title() {
        return title;
    }

    @Override
    public Publisher getSource() {
        return source;
    }

    public Topic venue() {
        return venue;
    }

    public DateTime startTime() {
        return startTime;
    }

    public DateTime endTime() {
        return endTime;
    }

    public ImmutableList<Person> participants() {
        return participants;
    }

    public ImmutableList<Organisation> organisations() {
        return organisations;
    }

    public ImmutableList<Topic> eventGroups() {
        return eventGroups;
    }

    public ImmutableList<ItemRef> content() {
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

    public Event copy() {
        Event event = Event.builder()
                .withTitle(title)
                .withSource(source)
                .withVenue(venue != null ? venue.copy() : null)
                .withStartTime(startTime)
                .withEndTime(endTime)
                .withParticipants(Iterables.transform(participants, Person::copy))
                .withOrganisations(Iterables.transform(organisations, Organisation::copy))
                .withEventGroups(Iterables.transform(eventGroups, Topic::copy))
                .withContent(Iterables.transform(content, ItemRef::copy))
                .build();

        Identified.copyTo(this, event);

        return event;
    }

    public static Builder<?> builder() {
        return new EventBuilder();
    }

    protected abstract static class Builder<T extends Builder<T>> extends Identified.Builder<T> {

        private String title;
        private Publisher source;
        private Topic venue;
        private DateTime startTime;
        private DateTime endTime;
        private ImmutableList<Person> participants = ImmutableList.of();
        private ImmutableList<Organisation> organisations = ImmutableList.of();
        private ImmutableList<Topic> eventGroups = ImmutableList.of();
        private ImmutableList<ItemRef> content = ImmutableList.of();

        private Builder() {}

        public T withTitle(String title) {
            this.title = title;
            return self();
        }

        public T withSource(Publisher publisher) {
            this.source = publisher;
            return self();
        }

        public T withVenue(Topic venue) {
            this.venue = venue;
            return self();
        }

        public T withStartTime(DateTime startTime) {
            this.startTime = startTime;
            return self();
        }

        public T withEndTime(DateTime endTime) {
            this.endTime = endTime;
            return self();
        }

        public T withParticipants(Iterable<Person> participants) {
            this.participants = ImmutableList.copyOf(participants);
            return self();
        }

        public T withOrganisations(Iterable<Organisation> organisations) {
            this.organisations = ImmutableList.copyOf(organisations);
            return self();
        }

        public T withEventGroups(Iterable<Topic> eventGroups) {
            this.eventGroups = ImmutableList.copyOf(eventGroups);
            return self();
        }

        public T withContent(Iterable<ItemRef> content) {
            this.content = ImmutableList.copyOf(content);
            return self();
        }

        public Event build() {
            return new Event(self());
        }
    }

    private static class EventBuilder extends Builder<EventBuilder> {

        @Override
        protected EventBuilder self() {
            return this;
        }
    }
}
