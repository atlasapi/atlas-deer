package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EventReadWriteWorkerTest {

    private @Mock EventResolver resolver;
    private @Mock EventWriter writer;
    private @Mock ResourceRef updatedResource;
    private @Mock Event event;

    private ResourceUpdatedMessage message;
    private SeparatingEventReadWriteWorker worker;

    @Before
    public void setUp() throws Exception {
        message = new ResourceUpdatedMessage("message", Timestamp.of(DateTime.now()), updatedResource);
        worker = SeparatingEventReadWriteWorker.create(
                resolver, writer, "prefix", new MetricRegistry(), null
        );

        Id id = Id.valueOf(0L);
        when(updatedResource.getId()).thenReturn(id);
        when(resolver.resolveIds(ImmutableList.of(id)))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(event))));
    }

    @Test
    public void testProcess() throws Exception {
        worker.process(message);

        verify(writer).write(event);
    }
}
