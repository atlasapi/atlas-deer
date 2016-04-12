package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.eventV2.EventV2Writer;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
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

    private @Mock EventV2Resolver resolver;
    private @Mock EventV2Writer writer;
    private @Mock Timer timer;
    private @Mock Timer.Context timerContext;
    private ResourceUpdatedMessage message;
    private @Mock ResourceRef updatedResource;
    private @Mock EventV2 event;

    private SeparatingEventReadWriteWorker worker;

    @Before
    public void setUp() throws Exception {
        message = new ResourceUpdatedMessage("message", Timestamp.of(DateTime.now()), updatedResource);
        worker = new SeparatingEventReadWriteWorker(resolver, writer, timer);

        Id id = Id.valueOf(0L);
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