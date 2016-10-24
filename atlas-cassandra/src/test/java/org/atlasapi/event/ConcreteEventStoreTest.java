package org.atlasapi.event;

import java.util.List;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConcreteEventStoreTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private @Captor ArgumentCaptor<Event> eventCaptor;
    private @Captor ArgumentCaptor<Iterable<Alias>> aliasCaptor;
    private @Captor ArgumentCaptor<ResourceUpdatedMessage> messageCaptor;

    private EventStore eventStore;

    private @Mock Clock clock;
    private @Mock IdGenerator idGenerator;
    private @Mock EventHasher eventHasher;
    private @Mock MessageSender<ResourceUpdatedMessage> sender;
    private @Mock EventPersistenceStore persistenceStore;
    private @Mock MetricRegistry metricRegistry;

    private DateTime now;
    private long id;
    private Publisher publisher;
    private String metricPrefix;

    @Before
    public void setUp() throws Exception {

        metricPrefix = "ConcreteEventStore";

        eventStore = new ConcreteEventStore(
                clock, idGenerator, eventHasher, sender, persistenceStore, metricRegistry, metricPrefix
        );

        now = DateTime.parse("2015-01-01T12:00:00.000Z");
        when(clock.now()).thenReturn(now);

        id = 12345L;
        when(idGenerator.generateRaw()).thenReturn(id);
        publisher = Publisher.BBC;
    }

    @Test
    public void testResolveIdsDelegatesToPersistenceStore() throws Exception {
        List<Id> ids = Lists.newArrayList(Id.valueOf(0L));
        eventStore.resolveIds(ids);

        verify(persistenceStore).resolveIds(ids);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWritePersistsNewEventIfNoneExists() throws Exception {
        when(persistenceStore.resolvePrevious(any(), any(), any()))
                .thenReturn(Optional.absent());

        Event expected = (Event) getEvent().build();
        eventStore.write(expected);

        verify(persistenceStore).write(eventCaptor.capture(), eq(null));

        Event actual = eventCaptor.getValue();
        assertThat(actual.getId().longValue(), is(id));
        assertThat(actual.getLastUpdated(), is(now));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getSource(), is(expected.getSource()));
    }

    @Test
    public void testWritePersistsNewEventUsesIdIfSpecified() throws Exception {
        when(persistenceStore.resolvePrevious(any(), any(), any()))
                .thenReturn(Optional.absent());

        Event expected = (Event) getEvent().withId(Id.valueOf(1111L)).build();
        eventStore.write(expected);

        verify(persistenceStore).write(eventCaptor.capture(), eq(null));

        Event actual = eventCaptor.getValue();
        assertThat(actual.getId().longValue(), is(1111L));
        assertThat(actual.getLastUpdated(), is(now));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getSource(), is(expected.getSource()));
    }

    @Test
    public void testWriteGetsIdFromExistingEvent() throws Exception {
        Id id = Id.valueOf(this.id);
        Alias alias = new Alias("ns", "value");
        Event previousEvent = (Event) getEvent()
                .withId(id)
                .withAliases(Lists.newArrayList(alias))
                .build();
        when(persistenceStore.resolvePrevious(any(), any(), any()))
                .thenReturn(Optional.of(previousEvent));
        when(eventHasher.hash(any())).thenReturn("a", "b");

        Event expected = (Event) getEvent()
                .withTitle("b")
                .withAliases(Lists.newArrayList(alias))
                .build();
        eventStore.write(expected);

        verify(persistenceStore).resolvePrevious(
                eq(Optional.absent()), eq(publisher), aliasCaptor.capture()
        );

        assertThat(aliasCaptor.getValue().iterator().hasNext(), is(true));
        assertThat(aliasCaptor.getValue().iterator().next(), is(alias));

        verify(persistenceStore).write(eventCaptor.capture(), eventCaptor.capture());

        assertThat(eventCaptor.getAllValues().size(), is(2));

        Event actual = eventCaptor.getAllValues().get(0);
        assertThat(actual.getId().longValue(), is(id.longValue()));
        assertThat(actual.getLastUpdated(), is(now));
        assertThat(actual.getTitle(), is("b"));

        Event actualPrevious = eventCaptor.getAllValues().get(1);
        assertThat(actualPrevious, sameInstance(previousEvent));
    }

    @Test
    public void testWriteSendsResourceUpdatedMessage() throws Exception {
        when(persistenceStore.resolvePrevious(any(), any(), any()))
                .thenReturn(Optional.absent());

        Event expected = (Event) getEvent().withId(Id.valueOf(1L)).build();
        eventStore.write(expected);

        verify(sender).sendMessage(
                messageCaptor.capture(), eq(Longs.toByteArray(expected.getId().longValue()))
        );

        ResourceUpdatedMessage actual = messageCaptor.getValue();
        assertThat(actual.getMessageId(), not(nullValue()));
        assertThat(actual.getTimestamp().toDateTimeUTC(), is(now.withZone(DateTimeZone.UTC)));
        assertThat(actual.getUpdatedResource().getId(), is(expected.getId()));
    }

    private Event.Builder getEvent() {
        return Event.builder()
                .withTitle("title")
                .withSource(publisher);
    }

}
