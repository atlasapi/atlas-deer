package org.atlasapi.system.bootstrap;

import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacySegmentMigrator;

import com.metabroadcast.common.collect.ImmutableOptionalMap;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentBootstrapListenerTest {

    private @Mock ContentWriter contentWriter;
    private @Mock DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
    private @Mock EquivalentContentStore equivalentContentStore;
    private @Mock ContentIndex contentIndex;
    private @Mock LegacySegmentMigrator legacySegmentMigrator;
    private @Mock ContentResolver legacyContentResolver;
    private @Mock EquivalenceGraphStore equivalenceGraphStore;

    private @Mock Item item;
    private ItemRef itemRef = new ItemRef(Id.valueOf(0L), Publisher.BBC, "", DateTime.now());

    private @Mock Brand brand;
    private BrandRef brandRef = new BrandRef(Id.valueOf(1L), Publisher.BBC);

    private @Mock SegmentEvent segmentEvent;
    private SegmentRef segmentRef = new SegmentRef(Id.valueOf(2L), Publisher.BBC);

    private @Mock Series series;
    private SeriesRef seriesRef = new SeriesRef(Id.valueOf(3L), Publisher.BBC);

    private @Mock Item seriesItem;
    private ItemRef seriesItemRef = new ItemRef(Id.valueOf(4L), Publisher.BBC, "", DateTime.now());

    private ContentBootstrapListener contentBootstrapListener;

    @Before
    public void setUp() throws Exception {
        when(item.getId()).thenReturn(itemRef.getId());

        contentBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentWriter)
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .withMigrateHierarchies(legacySegmentMigrator, legacyContentResolver)
                .build();
    }

    @Test
    public void testMigrateItem() throws Exception {
        mockItemBrandRef();
        EquivalenceGraphUpdate graphUpdate = mockContentMigration(item, itemRef);
        EquivalenceGraphUpdate brandGraphUpdate = mockContentMigration(brand, brandRef);

        when(segmentEvent.getSegmentRef()).thenReturn(segmentRef);
        when(item.getSegmentEvents()).thenReturn(Lists.newArrayList(segmentEvent));

        ContentBootstrapListener.Result result = contentBootstrapListener.visit(item);

        assertThat(result.getSucceeded(), is(true));

        verifyContentMigration(item, itemRef, graphUpdate);
        verifyContentMigration(brand, brandRef, brandGraphUpdate);
        verify(legacySegmentMigrator).migrateLegacySegment(segmentRef.getId());
    }

    @Test
    public void testMigrateBrand() throws Exception {
        EquivalenceGraphUpdate brandGraphUpdate = mockContentMigration(brand, brandRef);
        EquivalenceGraphUpdate seriesGraphUpdate = mockContentMigration(series, seriesRef);
        EquivalenceGraphUpdate seriesItemGraphUpdate = mockContentMigration(
                seriesItem,
                seriesItemRef
        );
        EquivalenceGraphUpdate itemGraphUpdate = mockContentMigration(item, itemRef);

        mockBrandSeries();
        mockBrandItemRefs();

        ContentBootstrapListener.Result result = contentBootstrapListener.visit(brand);

        assertThat(result.getSucceeded(), is(true));

        System.out.println(result);

        verifyContentMigration(brand, brandRef, brandGraphUpdate);
        verifyContentMigration(series, seriesRef, seriesGraphUpdate);
        verifyContentMigration(seriesItem, seriesItemRef, seriesItemGraphUpdate);
        verifyContentMigration(item, itemRef, itemGraphUpdate);
    }

    @Test
    public void testMigrateEquivalents() throws Exception {
        ContentBootstrapListener contentBootstrapListener = ContentBootstrapListener.builder()
                .withContentWriter(contentWriter)
                .withEquivalenceMigrator(equivalenceMigrator)
                .withEquivalentContentStore(equivalentContentStore)
                .withMigrateHierarchies(legacySegmentMigrator, legacyContentResolver)
                .withMigrateEquivalents(equivalenceGraphStore)
                .build();

        mockContentMigration(item, itemRef);
        EquivalenceGraphUpdate seriesItemGraphUpdate = mockContentMigration(
                seriesItem,
                seriesItemRef
        );

        when(item.getSegmentEvents()).thenReturn(ImmutableList.of());
        EquivalenceGraph equivalenceGraph = new EquivalenceGraph(
                ImmutableMap.of(
                        itemRef.getId(), EquivalenceGraph.Adjacents.valueOf(seriesItemRef),
                        seriesItemRef.getId(), EquivalenceGraph.Adjacents.valueOf(itemRef)
                ),
                DateTime.now()
        );
        when(equivalenceGraphStore.resolveIds(ImmutableList.of(itemRef.getId())))
                .thenReturn(Futures.immediateFuture(ImmutableOptionalMap.of(
                        itemRef.getId(), equivalenceGraph
                )));
        when(legacyContentResolver.resolveIds(ImmutableList.of(seriesItemRef.getId())))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(seriesItem))));

        ContentBootstrapListener.Result result = contentBootstrapListener.visit(item);

        assertThat(result.getSucceeded(), is(true));

        verifyContentMigration(seriesItem, seriesItemRef, seriesItemGraphUpdate);
    }

    private EquivalenceGraphUpdate mockContentMigration(Content content, ContentRef contentRef)
            throws Exception {
        EquivalenceGraphUpdate graphUpdate = mock(EquivalenceGraphUpdate.class);

        when(content.toRef()).thenReturn(contentRef);
        when(content.getId()).thenReturn(contentRef.getId());

        when(contentWriter.writeContent(content)).thenReturn(
                new WriteResult<>(content, true, DateTime.now(), null)
        );
        when(equivalenceMigrator.migrateEquivalence(contentRef)).thenReturn(
                Optional.of(graphUpdate)
        );

        return graphUpdate;
    }

    private void mockItemBrandRef() {
        when(item.getContainerRef()).thenReturn(brandRef);
        when(legacyContentResolver.resolveIds(Lists.newArrayList(brandRef.getId())))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(Lists.newArrayList(brand))));
    }

    private void mockBrandSeries() {
        when(brand.getSeriesRefs()).thenReturn(ImmutableList.of(seriesRef));
        when(legacyContentResolver.resolveIds(Lists.newArrayList(seriesRef.getId())))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(Lists.newArrayList(series))));

        when(series.getId()).thenReturn(seriesRef.getId());
        when(series.getItemRefs()).thenReturn(ImmutableList.of(seriesItemRef));
        when(legacyContentResolver.resolveIds(Lists.newArrayList(seriesItemRef.getId())))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(Lists.newArrayList(seriesItem))));
    }

    private void mockBrandItemRefs() {
        when(brand.getItemRefs()).thenReturn(ImmutableList.of(itemRef));
        when(legacyContentResolver.resolveIds(Lists.newArrayList(itemRef.getId()))).thenReturn(
                Futures.immediateFuture(Resolved.valueOf(Lists.newArrayList(item)))
        );
    }

    private void verifyContentMigration(Content content, ContentRef contentRef,
            EquivalenceGraphUpdate graphUpdate) throws Exception {
        verify(contentWriter).writeContent(content);
        verify(equivalenceMigrator).migrateEquivalence(contentRef);
        verify(equivalentContentStore).updateContent(content.getId());
        verify(equivalentContentStore).updateEquivalences(graphUpdate);
        verify(contentIndex).index(content);
    }
}
