package org.atlasapi.messaging;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.content.Brand;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EquivalentScheduleStoreContentUpdateWorkerTest {

    @Mock
    private EquivalentContentStore contentStore;

    @Mock
    private EquivalentScheduleWriter scheduleWriter;

    private EquivalentScheduleStoreContentUpdateWorker objectUnderTest;

    @Before
    public void setUp() {
        objectUnderTest = EquivalentScheduleStoreContentUpdateWorker.create(
                contentStore,
                scheduleWriter,
                "prefix",
                new MetricRegistry()
        );
    }

    @Test
    public void testProcess() throws Exception {
        Long equivalentSetId = 1L;
        Item item1 = new Item(Id.valueOf(1L), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now());
        Item item2 = new Item(Id.valueOf(2L), Publisher.METABROADCAST);

        when(contentStore.resolveEquivalentSet(equivalentSetId))
                .thenReturn(
                        Futures.immediateFuture(
                                ImmutableSet.of(item1, item2)
                        )
                );
        objectUnderTest.process(
                new EquivalentContentUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now()),
                        equivalentSetId,
                        item1.toRef()
                )
        );

        verify(scheduleWriter).updateContent(ImmutableSet.of(item1, item2));

    }

    @Test
    public void testFiltersOutNonItems() throws Exception {
        Long equivalentSetId = 1L;
        Item item1 = new Item(Id.valueOf(1L), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now());
        Item item2 = new Item(Id.valueOf(2L), Publisher.METABROADCAST);
        Brand brand = new Brand(Id.valueOf(3L), Publisher.METABROADCAST);

        when(contentStore.resolveEquivalentSet(equivalentSetId))
                .thenReturn(
                        Futures.immediateFuture(
                                ImmutableSet.of(item1, item2, brand)
                        )
                );
        objectUnderTest.process(
                new EquivalentContentUpdatedMessage(
                        UUID.randomUUID().toString(),
                        Timestamp.of(DateTime.now()),
                        equivalentSetId,
                        item1.toRef()
                )
        );

        verify(scheduleWriter).updateContent(ImmutableSet.of(item1, item2));
    }
}
