package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Person;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.ResolvedEvent;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationRef;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.query.v4.event.PersonListWriter;
import org.atlasapi.query.v4.organisation.OrganisationListWriter;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;


public class EventAnnotation extends OutputAnnotation<Event, ResolvedContent> { //TODO: possibly resolve organisations to ResolvedEvent

    private final EntityListWriter<ItemRef> itemRefWriter;
    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");
    private final EntityListWriter<Person> participantWriter = new PersonListWriter();
    private final EntityListWriter<Organisation> organisationListWriter = new OrganisationListWriter(
            participantWriter);

    private final OrganisationResolver resolver;

    public EventAnnotation(EntityListWriter<ItemRef> itemRefWriter, OrganisationResolver organisationResolver) {
        this.itemRefWriter = itemRefWriter;
        this.resolver = organisationResolver;
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        Event event = entity.getEvent();

        writer.writeField("title", event.getTitle());
        writer.writeField("start_time", event.getStartTime().toString());
        writer.writeField("end_time", event.getEndTime().toString());
        writer.writeObject(publisherWriter, event.getSource(), ctxt);
        writer.writeList(participantWriter, event.getParticipants(), ctxt);
        writer.writeList(organisationListWriter, resolveOrganisations(event.getOrganisations()), ctxt);
        writer.writeList(itemRefWriter, event.getContent(), ctxt);
    }

    private List<Organisation> resolveOrganisations(List<OrganisationRef> refs) {
        ImmutableList.Builder<Id> ids = ImmutableList.builder();
        for (OrganisationRef ref : refs) {
            ids.add(ref.getId());
        }
        try {
            Resolved<Organisation> resolved = resolver.resolveIds(ids.build()).get(30, TimeUnit.SECONDS);
            return resolved.getResources().toList();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }
}
