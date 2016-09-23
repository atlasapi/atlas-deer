package org.atlasapi.messaging;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.neo4j.service.Neo4jContentStore;

import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
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
public class Neo4jContentStoreContentUpdateWorkerTest {

    @Mock private ContentResolver contentResolver;
    @Mock private Neo4jContentStore neo4JContentStore;

    @Mock private ContentRef contentRef;
    @Mock private Content content;

    private EquivalentContentUpdatedMessage message;
    private Id id;

    private Neo4jContentStoreContentUpdateWorker worker;

    @Before
    public void setUp() throws Exception {
        id = Id.valueOf(0L);

        message = new EquivalentContentUpdatedMessage(
                "messageId", Timestamp.of(DateTime.now()), 10L, contentRef
        );

        when(contentRef.getId()).thenReturn(id);
        when(contentResolver.resolveIds(ImmutableList.of(id)))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(
                        ImmutableList.of(content)
                )));

        worker = Neo4jContentStoreContentUpdateWorker.create(
                contentResolver, neo4JContentStore, "", new MetricRegistry()
        );
    }

    @Test
    public void processMessageCallsDependenciesInOrder() throws Exception {
        worker.process(message);

        InOrder order = inOrder(contentResolver, neo4JContentStore);
        order.verify(contentResolver).resolveIds(ImmutableList.of(id));
        order.verify(neo4JContentStore).writeContent(content);
    }
}
