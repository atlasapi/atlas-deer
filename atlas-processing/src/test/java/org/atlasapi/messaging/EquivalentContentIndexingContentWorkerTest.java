package org.atlasapi.messaging;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.util.List;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.time.Timestamp;

@RunWith(MockitoJUnitRunner.class)
public class EquivalentContentIndexingContentWorkerTest {

    private EquivalentContentIndexingContentWorker worker;

    private @Mock ContentResolver contentResolver;
    private @Mock ContentIndex contentIndex;
    private @Mock MetricRegistry metricRegistry;
    private @Mock Timer timer;
    private @Mock Timer.Context timerContext;
    private @Mock Content content;

    @Before
    public void setUp() throws Exception {
        when(metricRegistry.timer(anyString())).thenReturn(timer);
        when(timer.time()).thenReturn(timerContext);

        worker = new EquivalentContentIndexingContentWorker(contentResolver, contentIndex, metricRegistry);
    }

    @Test
    public void testProcess() throws Exception {
        EquivalentContentUpdatedMessage message = new EquivalentContentUpdatedMessage(
                "messageId",
                Timestamp.of(DateTime.now(DateTimeZone.UTC)),
                111L,
                new ItemRef(Id.valueOf(222L), Publisher.BBC, "sortKey", DateTime.now(DateTimeZone.UTC))
        );

        List<Id> ids = Lists.newArrayList(message.getContentRef().getId());
        when(contentResolver.resolveIds(ids))
                .thenReturn(Futures.immediateFuture(new Resolved<>(Lists.newArrayList(content))));

        worker.process(message);

        InOrder order = inOrder(contentResolver, contentIndex, timer, timerContext);
        order.verify(timer).time();
        order.verify(contentResolver).resolveIds(ids);
        order.verify(contentIndex).index(content);
        order.verify(timerContext).stop();
    }
}