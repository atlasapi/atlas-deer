package org.atlasapi.event;

public class ResolvedEvent {

    private Event event;

    private ResolvedEvent(Builder builder) {
        event = builder.event;
    }

    public Event getEvent() {
        return event;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Event event;

        private Builder() {
        }

        public Builder withEvent(Event event) {
            this.event = event;
            return this;
        }

        public ResolvedEvent build() {
            return new ResolvedEvent(this);
        }
    }
}
