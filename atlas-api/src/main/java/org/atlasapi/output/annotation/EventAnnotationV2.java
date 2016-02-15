package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Person;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.eventV2.EventV2;
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

public class EventAnnotationV2 extends OutputAnnotation<EventV2> {

    private final EntityListWriter<ItemRef> itemRefWriter;
    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");
    private final EntityListWriter<Person> participantWriter = new PersonListWriter();
    private final EntityListWriter<Organisation> organisationListWriter = new OrganisationListWriter(
            participantWriter);

    private final OrganisationResolver resolver;

    public EventAnnotationV2(EntityListWriter<ItemRef> itemRefWriter, OrganisationResolver organisationResolver) {
        this.itemRefWriter = itemRefWriter;
        this.resolver = organisationResolver;
    }

    @Override
    public void write(EventV2 entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("title", entity.getTitle());
        writer.writeField("start_time", entity.getStartTime().toString());
        writer.writeField("end_time", entity.getEndTime().toString());
        writer.writeObject(publisherWriter, entity.getSource(), ctxt);
        writer.writeList(participantWriter, entity.getParticipants(), ctxt);
        writer.writeList(organisationListWriter, resolveOrganisations(entity.getOrganisations()), ctxt);
        writer.writeList(itemRefWriter, entity.getContent(), ctxt);
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
