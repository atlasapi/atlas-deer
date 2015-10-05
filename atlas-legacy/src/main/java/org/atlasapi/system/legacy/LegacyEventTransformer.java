package org.atlasapi.system.legacy;

import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.event.Event;

public class LegacyEventTransformer extends BaseLegacyResourceTransformer<
        org.atlasapi.media.entity.Event, Event> {

    private final LegacyTopicTransformer legacyTopicTransformer;
    private final LegacyPersonTransformer legacyPersonTransformer;
    private final LegacyOrganisationTransformer legacyOrganisationTransformer;

    public LegacyEventTransformer() {
        this.legacyTopicTransformer = new LegacyTopicTransformer();
        this.legacyPersonTransformer = new LegacyPersonTransformer();
        this.legacyOrganisationTransformer = new LegacyOrganisationTransformer();
    }

    @Nullable
    @Override
    public Event apply(org.atlasapi.media.entity.Event input) {
        Event.EventBuilder builder = Event.builder();

        addEvent(input, builder);
        Event event = builder.build();

        addIdentified(input, event);

        return event;
    }

    private void addEvent(org.atlasapi.media.entity.Event input, Event.EventBuilder builder) {
        builder.withTitle(input.title())
                .withSource(input.publisher())
                .withVenue(legacyTopicTransformer.apply(input.venue()))
                .withStartTime(input.startTime())
                .withEndTime(input.endTime())
                .withParticipants(input.participants().stream()
                        .map(legacyPersonTransformer::apply)
                        .collect(Collectors.toList()))
                .withOrganisations(input.organisations().stream()
                        .map(legacyOrganisationTransformer::apply)
                        .collect(Collectors.toList()))
                .withEventGroups(input.eventGroups().stream()
                        .map(legacyTopicTransformer::apply)
                        .collect(Collectors.toList()))
                .withContent(input.content().stream()
                        .map(ref -> LegacyContentTransformer.legacyRefToRef(ref, input.publisher()))
                        .collect(Collectors.toList()));
    }
}
