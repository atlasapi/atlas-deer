package org.atlasapi.content;

import org.atlasapi.event.Event;

public class ResolvedContent {

    private Content content;
    private Described described;
    private Event event;

    private ResolvedContent(Builder builder) {
        content = builder.content;
        described = builder.described;
        event = builder.event;
    }

    public Content getContent() {
        return content;
    }

    public Described getDescribed() {
        return described;
    }

    public Event getEvent() {
        return event;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Content content;
        private Described described;
        private Event event;

        private Builder() {
        }

        public Builder withContent(Content content) {
            this.content = content;
            return this;
        }

        public Builder withDescribed(Described described) {
            this.described = described;
            return this;
        }

        public Builder withEvent(Event event) {
            this.event = event;
            return this;
        }

        public ResolvedContent build() {
            return new ResolvedContent(this);
        }
    }

    public static ResolvedContent wrap(Content content) {
        return ResolvedContent.builder().withContent(content).build();
    }

    public static ResolvedContent wrap(Described desc) {
        return ResolvedContent.builder().withDescribed(desc).build();
    }

    public static ResolvedContent wrap(Event event) {
        return ResolvedContent.builder().withEvent(event).build();
    }
}
