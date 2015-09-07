package org.atlasapi.event;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;
import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class Event extends Identified {

    private String title;
    private Publisher publisher;
    private Topic venue;
    private DateTime startTime;
    private DateTime endTime;
    private ImmutableList<Person> participants;
    private ImmutableList<Organisation> organisations;
    private ImmutableList<Topic> eventGroups;
    private ImmutableList<ItemRef> content;

    public static Builder builder() {
        return new Builder();
    }

    public Event(String title, Publisher publisher, @Nullable Topic venue,
            @Nullable DateTime startTime, @Nullable DateTime endTime, Iterable<Person> participants,
            Iterable<Organisation> organisations, Iterable<Topic> eventGroups,
            Iterable<ItemRef> content) {
        this.title = checkNotNull(title);
        this.publisher = checkNotNull(publisher);
        this.venue = venue;
        this.startTime = startTime;
        this.endTime = endTime;
        this.participants = ImmutableList.copyOf(participants);
        this.organisations = ImmutableList.copyOf(organisations);
        this.eventGroups = ImmutableList.copyOf(eventGroups);
        this.content = ImmutableList.copyOf(content);
    }

    public String title() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Publisher publisher() {
        return publisher;
    }

    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    public Topic venue() {
        return venue;
    }

    public void setVenue(Topic venue) {
        this.venue = venue;
    }

    public DateTime startTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public DateTime endTime() {
        return endTime;
    }

    public void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    public ImmutableList<Person> participants() {
        return participants;
    }

    public void setParticipants(Iterable<Person> participants) {
        this.participants = ImmutableList.copyOf(participants);
    }

    public ImmutableList<Organisation> organisations() {
        return organisations;
    }

    public void setOrganisations(Iterable<Organisation> organisations) {
        this.organisations = ImmutableList.copyOf(organisations);
    }

    public ImmutableList<Topic> eventGroups() {
        return eventGroups;
    }

    public void setEventGroups(Iterable<Topic> eventGroups) {
        this.eventGroups = ImmutableList.copyOf(eventGroups);
    }

    public ImmutableList<ItemRef> content() {
        return content;
    }

    public void setContent(Iterable<ItemRef> content) {
        this.content = ImmutableList.copyOf(content);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(Event.class)
                .add("title", title)
                .add("publisher", publisher)
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
                .withPublisher(publisher)
                .withVenue(venue.copy())
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

    public static class Builder {

        private String title;
        private Publisher publisher;
        private Topic venue;
        private DateTime startTime;
        private DateTime endTime;
        private ImmutableList.Builder<Person> participants = ImmutableList.builder();
        private ImmutableList.Builder<Organisation> organisations = ImmutableList.builder();
        private ImmutableList.Builder<Topic> eventGroups = ImmutableList.builder();
        private ImmutableList.Builder<ItemRef> content = ImmutableList.builder();

        private Builder() {}

        public Event build() {
            return new Event(title, publisher, venue, startTime, endTime, participants.build(),
                    organisations.build(), eventGroups.build(), content.build());
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withVenue(Topic venue) {
            this.venue = venue;
            return this;
        }

        public Builder withStartTime(DateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder withEndTime(DateTime endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder withParticipants(Iterable<Person> participants) {
            this.participants.addAll(participants);
            return this;
        }

        public Builder withOrganisations(Iterable<Organisation> organisations) {
            this.organisations.addAll(organisations);
            return this;
        }

        public Builder withEventGroups(Iterable<Topic> eventGroups) {
            this.eventGroups.addAll(eventGroups);
            return this;
        }

        public Builder withContent(Iterable<ItemRef> content) {
            this.content.addAll(content);
            return this;
        }
    }
}
