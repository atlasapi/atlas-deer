package org.atlasapi.system.bootstrap.workers;

import java.util.Collection;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.system.bootstrap.ColumbusTelescopeReporter;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.TelescopeClientImpl;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentBootstrapWorkerTest {

    private static final String TEST_RAW_STRING = "{ \"test\":\"test\"}";

    @Captor private ArgumentCaptor<Event> columbusTelescopeEventCaptor;

    @Mock private ContentResolver contentResolver;
    @Mock private ContentWriter writer;
    @Mock private TelescopeClientImpl telescopeClient;
    @Mock private ResourceRef resourceRef;
    @Mock private ResourceUpdatedMessage resourceUpdatedMessage;
    @Mock private ListenableFuture<Resolved<Content>> listenableFuture;
    @Mock private ObjectMapper objectMapper;

    private ContentBootstrapWorker bootstrapWorker;
    private WriteResult<Content, Content> successfulWriteResult;
    private Content content;

    @Before
    public void setUp() throws Exception {
        ColumbusTelescopeReporter columbusTelescopeReporter = ColumbusTelescopeReporter.create(
                telescopeClient,
                "PRODUCTION",
                objectMapper
        );

        this.bootstrapWorker = ContentBootstrapWorker.create(
                contentResolver,
                writer,
                "prefix",
                new MetricRegistry(),
                columbusTelescopeReporter
        );
        this.content = new org.atlasapi.content.Item("panda.com", "panda", Publisher.PA);
        this.content.setId(Id.valueOf(1L));
        this.successfulWriteResult = new WriteResult<>(content, true, DateTime.now(), null);
    }

    @Test
    public void testReportingSuccessfulMigrationEvent() throws RecoverableException, WriteException {
        ResourceUpdatedMessage updatedMessage = createUpdatedMessage();

        when(resourceRef.getId()).thenReturn(Id.valueOf(1));
        when(contentResolver.resolveIds(any(Collection.class)))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(content))));
        when(writer.writeContent(content)).thenReturn(successfulWriteResult);

        bootstrapWorker.process(updatedMessage);

        verify(telescopeClient, times(1)).createEvent(any(Event.class));
        verify(telescopeClient).createEvent(columbusTelescopeEventCaptor.capture());

        Event event = columbusTelescopeEventCaptor.getValue();

        assertThat(event.getStatus(), is(Event.Status.SUCCESS));
        assertThat(event.getType(), is(Event.Type.MIGRATION));
    }

    @Test
    public void testReportingMigrationCorrectRawObjectIsSentAsPartOfTheEventReport()
            throws RecoverableException, WriteException, JsonProcessingException {
        this.content.setLastFetched(DateTime.parse("2016-08-28T00:00:00Z"));
        ResourceUpdatedMessage updatedMessage = createUpdatedMessage();

        when(resourceRef.getId()).thenReturn(Id.valueOf(1));
        when(contentResolver.resolveIds(any(Collection.class)))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(content))));
        when(writer.writeContent(content)).thenReturn(successfulWriteResult);
        when(objectMapper.writeValueAsString(any(Content.class))).thenReturn(TEST_RAW_STRING);

        bootstrapWorker.process(updatedMessage);

        verify(telescopeClient, times(1)).createEvent(any(Event.class));
        verify(telescopeClient).createEvent(columbusTelescopeEventCaptor.capture());

        Event event = columbusTelescopeEventCaptor.getValue();
        assertThat(event.getEntityState().get().getRaw().get(), is(TEST_RAW_STRING));
        assertThat(event.getEntityState().get().getRawMime().get(), is(MimeType.APPLICATION_JSON.toString()));
        assertThat(event.getEntityState().get().getAtlasId().get(), is("c"));
    }

    private ResourceUpdatedMessage createUpdatedMessage() {
        return new ResourceUpdatedMessage(
                    "panda",
                    Timestamp.of(DateTime.now()),
                    new ResourceRef(Id.valueOf(1L), Publisher.PA) {

                        @Override
                        public ResourceType getResourceType() {
                            return ResourceType.CONTENT;
                        }
                    }
            );
    }
}
