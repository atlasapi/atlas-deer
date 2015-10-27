package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Person;
import org.atlasapi.event.Event;
import org.atlasapi.event.Organisation;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.v4.event.OrganisationListWriter;
import org.atlasapi.query.v4.event.PersonListWriter;
import org.atlasapi.query.v4.topic.TopicListWriter;

import org.atlasapi.topic.Topic;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Matchers.*;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class EventAnnotationTest {

    private @Mock FieldWriter fieldWriter;
    private NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private EntityListWriter<Topic> topicListWriter = new TopicListWriter(AnnotationRegistry.<Topic>builder()
            .build());
    private EntityListWriter<ItemRef> itemRefWriter = new ItemRefWriter(idCodec, "items");
    private final EventAnnotation eventAnnotation = new EventAnnotation(topicListWriter, itemRefWriter);

    @Test
    public void eventFieldsTest() throws Exception {
        Topic topic = new Topic();
        topic.setId(Id.valueOf(1234));
        topic.setPublisher(Publisher.BBC);

        ItemRef itemRef = new ItemRef(Id.valueOf("1234"), Publisher.BBC, "12345", new DateTime());

        Person person = new Person();
        person.setId(Id.valueOf(12));
        person.setPublisher(Publisher.BBC);

        Organisation organisation = new Organisation();
        organisation.setPublisher(Publisher.BBC);
        organisation.setId(Id.valueOf(14));

        Event event = Event.builder().withTitle("Sherlock Holmes")
                .withSource(Publisher.BBC)
                .withStartTime(new DateTime())
                .withEndTime(new DateTime())
                .withEventGroups(ImmutableSet.of(topic))
                .withContent(ImmutableSet.of(itemRef))
                .withParticipants(ImmutableSet.of(person))
                .withVenue(topic)
                .withOrganisations(ImmutableSet.of(organisation))
                .build();

        OutputContext context = OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)));

        eventAnnotation.write(event, fieldWriter, context);
        verify(fieldWriter).writeField("title", event.getTitle());
        verify(fieldWriter).writeField("start_time", event.getStartTime().toString());
        verify(fieldWriter).writeField("end_time", event.getEndTime().toString());
        verify(fieldWriter).writeObject(isA(TopicListWriter.class), eq("venue"), isA(Topic.class), any());
        verify(fieldWriter).writeList(isA(PersonListWriter.class), anyCollectionOf(Person.class), any());
        verify(fieldWriter).writeList(isA(OrganisationListWriter.class), anyCollectionOf(Organisation.class), any());
        verify(fieldWriter).writeList(isA(ItemRefWriter.class), anyCollectionOf(ItemRef.class), any());

    }

}
