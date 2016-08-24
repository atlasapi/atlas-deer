package org.atlasapi.messaging;

import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceAssertion;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Neo4jContentStoreGraphUpdateWorkerTest {

    @Mock private ContentResolver legacyResolver;
    @Mock private LookupEntryStore legacyEquivalenceStore;
    @Mock private Neo4jContentStore neo4JContentStore;
    @Mock private Timer timer;
    @Mock private Meter failureMeter;
    @Mock private Timer.Context timerContext;

    @Mock private EquivalenceGraphUpdate graphUpdate;
    @Mock private LookupEntry lookupEntry;

    private EquivalenceGraphUpdateMessage message;
    private EquivalenceAssertion assertion;

    private Neo4jContentStoreGraphUpdateWorker worker;
    private Item explicitEquivalentItem;
    private Item directEquivalentItem;

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

        when(
                legacyEquivalenceStore.entriesForIds(
                        ImmutableList.of(assertion.getSubject().getId().longValue())
                )
        )
                .thenReturn(ImmutableList.of(lookupEntry));

        LookupRef explicitEquivalent = new LookupRef(
                "uriA", 0L, Publisher.BBC, ContentCategory.TOP_LEVEL_ITEM
        );
        LookupRef directEquivalent = new LookupRef(
                "uriB", 1L, Publisher.METABROADCAST, ContentCategory.TOP_LEVEL_ITEM
        );

        when(lookupEntry.explicitEquivalents()).thenReturn(ImmutableSet.of(explicitEquivalent));
        when(lookupEntry.directEquivalents()).thenReturn(ImmutableSet.of(directEquivalent));

        explicitEquivalentItem = new Item(
                Id.valueOf(explicitEquivalent.id()), Publisher.BBC
        );
        explicitEquivalentItem.setThisOrChildLastUpdated(DateTime.now());
        directEquivalentItem = new Item(
                Id.valueOf(directEquivalent.id()), Publisher.METABROADCAST
        );
        directEquivalentItem.setThisOrChildLastUpdated(DateTime.now());

        when(legacyResolver.resolveIds(ImmutableSet.of(
                Id.valueOf(explicitEquivalent.id()), Id.valueOf(directEquivalent.id()))
        ))
                .thenReturn(Futures.immediateFuture(
                        Resolved.valueOf(ImmutableList.of(
                                explicitEquivalentItem,
                                directEquivalentItem
                        ))
                ));

        worker = Neo4jContentStoreGraphUpdateWorker.create(
                legacyResolver,
                legacyEquivalenceStore,
                neo4JContentStore,
                timer,
                failureMeter
        );
    }

    @Test
    public void processMessageCallsDependenciesInOrder() throws Exception {
        when(
                legacyEquivalenceStore.entriesForIds(
                        ImmutableList.of(assertion.getSubject().getId().longValue())
                )
        )
                .thenReturn(ImmutableList.of(lookupEntry));

        worker.process(message);

        InOrder order = inOrder(timer, timerContext, neo4JContentStore);
        order.verify(timer).time();
        //noinspection unchecked
        order.verify(neo4JContentStore).writeEquivalences(
                any(ResourceRef.class),
                anySet(),
                anySet()
        );
        order.verify(timerContext).stop();
    }

    @Test
    public void processMessageResolvedAdjacentsFromLookupStore() throws Exception {
        when(
                legacyEquivalenceStore.entriesForIds(
                        ImmutableList.of(assertion.getSubject().getId().longValue())
                )
        )
                .thenReturn(ImmutableList.of(lookupEntry));

        worker.process(message);

        verify(neo4JContentStore).writeEquivalences(
                assertion.getSubject(),
                ImmutableSet.of(explicitEquivalentItem.toRef(), directEquivalentItem.toRef()),
                Publisher.all()
        );
    }

    @Test
    public void processMessageIgnoresAdjacentsAndSourcesInMessage() throws Exception {
        when(
                legacyEquivalenceStore.entriesForIds(
                        ImmutableList.of(assertion.getSubject().getId().longValue())
                )
        )
                .thenReturn(ImmutableList.of(lookupEntry));

        worker.process(message);

        verify(neo4JContentStore, never()).writeEquivalences(
                assertion.getSubject(),
                assertion.getAssertedAdjacents(),
                assertion.getSources()
        );
    }
}
