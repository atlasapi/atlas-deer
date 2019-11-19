package org.atlasapi.equivalence;

import java.util.Optional;

import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EquivalenceGraphFilterTest {

    private Item bbcItem;
    private Item c4Item;
    private Item huluItem;
    private Item paItem;
    private Item tedItem;
    private Item vimeoItem;
    private Item itvItem;

    /**
     *  BBC --|
     *        |
     *  C4 -----> PA <----- TED <----- VIMEO
     *        |                  |
     *  HULU -|                  |--> ITV
     */
    private EquivalenceGraph graph;

    @Before
    public void setUp() throws Exception {
        long id = 0L;

        bbcItem = getItem(id++, Publisher.BBC);
        c4Item = getItem(id++, Publisher.C4);
        huluItem = getItem(id++, Publisher.HULU);
        paItem = getItem(id++, Publisher.PA);
        tedItem = getItem(id++, Publisher.TED);
        vimeoItem = getItem(id++, Publisher.VIMEO);
        itvItem = getItem(id, Publisher.ITV);

        graph = makeGraph();
    }

    @Test
    public void whenAllSourcesAreSelectedMatchEverything() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(paItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(Publisher.all())
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(
                filter,
                ImmutableSet.of(
                        bbcItem,
                        c4Item,
                        huluItem,
                        paItem,
                        tedItem,
                        vimeoItem,
                        itvItem
                )
        );
        assertUnreachable(filter, ImmutableSet.of());
    }

    @Test
    public void whenSourceIsNotSelectedReturnItButNotChildren() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(paItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(
                        Sets.difference(
                                Publisher.all(),
                                ImmutableSet.of(Publisher.TED)
                        )
                )
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(filter, ImmutableSet.of(bbcItem, c4Item, huluItem, paItem, tedItem));
        assertUnreachable(filter, ImmutableSet.of(vimeoItem, itvItem));
    }

    @Test
    public void whenEntrySourceIsNotSelectedOnlyReturnEntryNode() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(paItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(ImmutableSet.of())
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(filter, ImmutableSet.of(paItem));
        assertUnreachable(filter, ImmutableSet.of());
    }

    @Test
    public void whenSourceIsSelectedReturnOutgoingEdges() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(vimeoItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(ImmutableSet.of(Publisher.VIMEO))
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(filter, ImmutableSet.of(vimeoItem, tedItem));
    }

    @Test
    public void whenSourceIsSelectedReturnIncomingEdges() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(tedItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(ImmutableSet.of(Publisher.ITV))
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(filter, ImmutableSet.of(itvItem, tedItem));
    }

    @Ignore
    @Test
    public void whenIdIsNotActivelyPublishedDoNotReturnItOrItsChildren() throws Exception {
        tedItem.setActivelyPublished(false);

        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(paItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(Publisher.all())
                .withIds(
                        Sets.difference(
                                graph.getEquivalenceSet(),
                                ImmutableSet.of(tedItem.getId())
                        )
                )
                .build();

        assertReachable(filter, ImmutableSet.of(bbcItem, c4Item, huluItem, paItem));
        assertUnreachable(filter, ImmutableSet.of(tedItem, vimeoItem, itvItem));
    }

    @Test
    public void whenEntryPointIsNotActivelyPublishedReturnNothing() throws Exception {
        tedItem.setActivelyPublished(false);

        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(tedItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(Publisher.all())
                .withSelectedGraphSources(Publisher.all())
                .withIds(
                        Sets.difference(
                                graph.getEquivalenceSet(),
                                ImmutableSet.of(tedItem.getId())
                        )
                )
                .build();

        assertReachable(filter, ImmutableSet.of());
        assertUnreachable(
                filter,
                ImmutableSet.of(
                        bbcItem,
                        c4Item,
                        huluItem,
                        paItem,
                        tedItem,
                        vimeoItem,
                        itvItem
                )
        );
    }

    @Test
    public void whenContentSourceIsNotSelectedDoNotReturnIt() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(tedItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(ImmutableSet.of(Publisher.PA))
                .withSelectedGraphSources(Publisher.all())
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(filter, ImmutableSet.of(paItem));
        assertUnreachable(
                filter,
                ImmutableSet.of(
                        bbcItem,
                        c4Item,
                        huluItem,
                        tedItem,
                        vimeoItem,
                        itvItem
                )
        );
    }

    @Test
    public void selectedSourcesDoesNotAffectChildrenDiscovery() throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(tedItem.getId()))
                .withGraph(Optional.of(graph))
                .withSelectedSources(ImmutableSet.of(Publisher.VIMEO))
                .withSelectedGraphSources(Publisher.all())
                .withIds(graph.getEquivalenceSet())
                .build();

        assertReachable(filter, ImmutableSet.of(vimeoItem));
        assertUnreachable(
                filter,
                ImmutableSet.of(
                        bbcItem,
                        c4Item,
                        huluItem,
                        paItem,
                        tedItem,
                        itvItem
                )
        );
    }

    @Test
    public void ifNoGraphHasBeenGivenFilterOnlyByActivelyPublishedAndSelectedSources()
            throws Exception {
        EquivalenceGraphFilter filter = EquivalenceGraphFilter.builder()
                .withGraphEntryId(Optional.of(paItem.getId()))
                .withGraph(Optional.empty())
                .withSelectedSources(
                        Sets.difference(
                                Publisher.all(),
                                ImmutableSet.of(Publisher.BBC)
                        )
                )
                .withSelectedGraphSources(ImmutableSet.of())
                .withIds(Sets.difference(
                        graph.getEquivalenceSet(),
                        ImmutableSet.of(tedItem.getId()))
                )
                .build();

        assertReachable(
                filter,
                ImmutableSet.of(
                        c4Item,
                        huluItem,
                        paItem,
                        itvItem,
                        vimeoItem
                )
        );
        assertUnreachable(
                filter,
                ImmutableSet.of(
                        bbcItem,
                        tedItem
                )
        );
    }

    private Item getItem(long id, Publisher source) {
        Item item = new Item(Id.valueOf(id), source);
        item.setThisOrChildLastUpdated(DateTime.now());

        return item;
    }

    private EquivalenceGraph makeGraph() {
        ImmutableSet<EquivalenceGraph.Adjacents> adjacents = ImmutableSet.of(
                getAdjacents(
                        bbcItem.toRef(),
                        ImmutableList.of(bbcItem.toRef(), paItem.toRef()),
                        ImmutableList.of(bbcItem.toRef())
                ),
                getAdjacents(
                        c4Item.toRef(),
                        ImmutableList.of(c4Item.toRef(), paItem.toRef()),
                        ImmutableList.of(c4Item.toRef())
                ),
                getAdjacents(
                        huluItem.toRef(),
                        ImmutableList.of(huluItem.toRef(), paItem.toRef()),
                        ImmutableList.of(huluItem.toRef())
                ),
                getAdjacents(
                        paItem.toRef(),
                        ImmutableList.of(
                                paItem.toRef(),
                                bbcItem.toRef(),
                                c4Item.toRef(),
                                huluItem.toRef(),
                                tedItem.toRef()
                        ),
                        ImmutableList.of(paItem.toRef())
                ),
                getAdjacents(
                        tedItem.toRef(),
                        ImmutableList.of(tedItem.toRef(), paItem.toRef(), itvItem.toRef()),
                        ImmutableList.of(tedItem.toRef(), vimeoItem.toRef())
                ),
                getAdjacents(
                        vimeoItem.toRef(),
                        ImmutableList.of(vimeoItem.toRef(), tedItem.toRef()),
                        ImmutableList.of(vimeoItem.toRef())
                ),
                getAdjacents(
                        itvItem.toRef(),
                        ImmutableList.of(itvItem.toRef()),
                        ImmutableList.of(itvItem.toRef(), tedItem.toRef())
                )
        );

        return EquivalenceGraph.valueOf(adjacents);
    }

    private EquivalenceGraph.Adjacents getAdjacents(
            ItemRef item,
            ImmutableList<ResourceRef> outgoing,
            ImmutableList<ResourceRef> incoming
    ) {
        return EquivalenceGraph.Adjacents.valueOf(item)
                .copyWithOutgoing(outgoing)
                .copyWithIncoming(incoming);
    }

    private void assertReachable(
            EquivalenceGraphFilter filter,
            ImmutableSet<Item> reachable
    ) {
        assertThat(
                reachable.stream().allMatch(filter),
                is(true)
        );
    }

    private void assertUnreachable(
            EquivalenceGraphFilter filter,
            ImmutableSet<Item> unreachable
    ) {
        assertThat(
                unreachable.stream().noneMatch(filter),
                is(true)
        );
    }
}
