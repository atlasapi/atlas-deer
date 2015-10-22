package org.atlasapi.output.annotation;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Person;
import org.atlasapi.event.Event;
import org.atlasapi.event.Organisation;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.query.v4.event.OrganisationListWriter;
import org.atlasapi.query.v4.event.PersonListWriter;
import org.atlasapi.topic.Topic;

import java.io.IOException;

public class EventAnnotation extends OutputAnnotation<Event> {

    private final EntityListWriter<Topic> topicWriter;
    private final EntityListWriter<ItemRef> itemRefWriter;
    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");
    private final EntityListWriter<Person> participantWriter = new PersonListWriter();
    private final EntityListWriter<Organisation> organisationListWriter = new OrganisationListWriter(participantWriter);

    public EventAnnotation(EntityListWriter<Topic> topicWriter, EntityListWriter<ItemRef> itemRefWriter) {
        this.topicWriter = topicWriter;
        this.itemRefWriter = itemRefWriter;
    }

    @Override
    public void write(Event entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("title", entity.getTitle());
        writer.writeField("start_time", entity.getStartTime().toString());
        writer.writeField("end_time", entity.getEndTime().toString());
        writer.writeObject(publisherWriter, entity.getSource(), ctxt);
        writer.writeObject(topicWriter, "venue", entity.getVenue(), ctxt);
        writer.writeList(participantWriter, entity.getParticipants(), ctxt);
        writer.writeList(topicWriter, entity.getEventGroups(), ctxt);
        writer.writeList(organisationListWriter, entity.getOrganisations(), ctxt);
        writer.writeList(itemRefWriter, entity.getContent(), ctxt);
    }
}
