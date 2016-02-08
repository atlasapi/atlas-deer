package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
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
    private @Mock Timer timer;
    private @Mock Timer.Context timerContext;
    private @Mock ResourceUpdatedMessage message;
    private @Mock ResourceRef updatedResource;
    private @Mock Event event;

    private EventReadWriteWorker worker;

    @Before
    public void setUp() throws Exception {
        worker = new EventReadWriteWorker(resolver, writer, timer);

        Id id = Id.valueOf(0L);
        when(message.getUpdatedResource()).thenReturn(updatedResource);
        when(updatedResource.getId()).thenReturn(id);
        when(resolver.resolveIds(ImmutableList.of(id)))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(event))));
        when(timer.time()).thenReturn(timerContext);
    }

    @Test
    public void testProcess() throws Exception {
        worker.process(message);

        verify(writer).write(event);
    }
}