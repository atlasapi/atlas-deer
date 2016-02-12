package org.atlasapi.system.legacy;

import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.eventV2.EventV2;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationRef;
import org.atlasapi.organisation.OrganisationStore;

public class LegacyEventV2Transformer extends BaseLegacyResourceTransformer<
        org.atlasapi.media.entity.Event, EventV2> {

    private final LegacyTopicTransformer legacyTopicTransformer;
    private final LegacyPersonTransformer legacyPersonTransformer;
    private final LegacyOrganisationTransformer legacyOrganisationTransformer;
    private final OrganisationStore organisationStore;

    public LegacyEventV2Transformer(OrganisationStore organisationStore) {
        this.legacyTopicTransformer = new LegacyTopicTransformer();
        this.legacyPersonTransformer = new LegacyPersonTransformer();
        this.legacyOrganisationTransformer = new LegacyOrganisationTransformer();
        this.organisationStore = organisationStore;
    }

    @Nullable
    @Override
    public EventV2 apply(org.atlasapi.media.entity.Event input) {
        EventV2.EventBuilder builder = EventV2.builder();

        addEvent(input, builder);
        EventV2 event = builder.build();

        addIdentified(input, event);

        return event;
    }

    private void addEvent(org.atlasapi.media.entity.Event input, EventV2.EventBuilder builder) {
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
                        .map(this::generateOrganisationRef)
                        .collect(Collectors.toList()))
                .withEventGroups(input.eventGroups().stream()
                        .map(legacyTopicTransformer::apply)
                        .collect(Collectors.toList()))
                .withContent(input.content().stream()
                        .map(ref -> LegacyContentTransformer.legacyRefToRef(ref, input.publisher()))
                        .collect(Collectors.toList()));
    }

    private OrganisationRef generateOrganisationRef(Organisation organisation) {
        Organisation writtenOrganisation = writeOrganisation(organisation);
        return new OrganisationRef(writtenOrganisation.getId(), organisation.getSource());
    }

    private Organisation writeOrganisation(Organisation organisation) {
        return organisationStore.write(organisation);
    }

}
