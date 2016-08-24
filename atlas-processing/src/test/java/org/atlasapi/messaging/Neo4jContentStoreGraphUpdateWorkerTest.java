package org.atlasapi.messaging;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceAssertion;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.Neo4jContentStore;

import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Neo4jContentStoreGraphUpdateWorkerTest {

    @Mock private Neo4jContentStore neo4JContentStore;
    @Mock private Timer timer;
    @Mock private Meter failureMeter;
    @Mock private Timer.Context timerContext;

    @Mock private EquivalenceGraphUpdate graphUpdate;

    private EquivalenceGraphUpdateMessage message;
    private EquivalenceAssertion assertion;

    private Neo4jContentStoreGraphUpdateWorker worker;

    @Before
    public void setUp() throws Exception {
        message = new EquivalenceGraphUpdateMessage(
                "messageId",
                Timestamp.of(DateTime.now()),
                graphUpdate
        );

        assertion = EquivalenceAssertion.create(
                new ItemRef(Id.valueOf(0L), Publisher.BBC, "", DateTime.now()),
                ImmutableList.of(
                        new ItemRef(Id.valueOf(1L), Publisher.BBC, "", DateTime.now())
                ),
                ImmutableList.of(
                        Publisher.BBC
                )
        );

        when(graphUpdate.getAssertion()).thenReturn(assertion);

        when(timer.time()).thenReturn(timerContext);

        worker = Neo4jContentStoreGraphUpdateWorker.create(neo4JContentStore, timer, failureMeter);
    }

    @Test
    public void processMessageCallsDependenciesInOrder() throws Exception {
        worker.process(message);

        InOrder order = inOrder(timer, timerContext, neo4JContentStore);
        order.verify(timer).time();
        order.verify(neo4JContentStore).writeEquivalences(
                assertion.getSubject(),
                assertion.getAssertedAdjacents(),
                assertion.getSources()
        );
        order.verify(timerContext).stop();
    }
}
