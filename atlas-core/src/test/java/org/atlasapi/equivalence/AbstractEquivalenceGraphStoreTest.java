package org.atlasapi.equivalence;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourced;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.GroupLock;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AbstractEquivalenceGraphStoreTest {

    private final Item bbcItem = new Item(Id.valueOf(1), Publisher.BBC);
    private final Item paItem = new Item(Id.valueOf(2), Publisher.PA);
    private final Item itvItem = new Item(Id.valueOf(3), Publisher.ITV);
    private final Item c4Item = new Item(Id.valueOf(4), Publisher.C4);
    private final Item fiveItem = new Item(Id.valueOf(5), Publisher.FIVE);

    private final InMemoryEquivalenceGraphStore store = new InMemoryEquivalenceGraphStore();

    @Before
    public void setup() {
        bbcItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        paItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        itvItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        c4Item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        fiveItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
    }

    @After
    public void tearDown() {
        store.store.clear();
    }

    @Test
    public void testMakingTwoResourcesEquivalent() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);
    }

    private EquivalenceGraph graphOf(Item item) {
        return get(store.resolveIds(ImmutableList.of(item.getId()))).get(item.getId()).get();
    }

    @Test
    public void testMakingThreeResourcesEquivalent() throws WriteException {
        makeEquivalent(bbcItem, paItem, c4Item);

        assertOutgoingAdjacents(bbcItem, paItem, c4Item);
        assertIncomingAdjacent(paItem, bbcItem);
        assertIncomingAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
    }

    @Test
    public void testAddingAnEquivalentResource() throws WriteException {
        makeEquivalent(bbcItem, paItem);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);

        makeEquivalent(bbcItem, paItem, c4Item);

        assertOutgoingAdjacents(bbcItem, paItem, c4Item);
        assertIncomingAdjacent(paItem, bbcItem);
        assertIncomingAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
    }

    @Test
    public void testThatChangingTypeOfItemDoesntChangeEquivalence() throws WriteException {
        Item subject = new Item(Id.valueOf(10), Publisher.AMAZON_UK);
        subject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(subject, paItem);
        Episode changedSubject = new Episode(Id.valueOf(10), Publisher.AMAZON_UK);
        changedSubject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(changedSubject, paItem);

        assertIncomingAdjacent(paItem, subject);
        assertIncomingAdjacent(paItem, changedSubject);
        assertOutgoingAdjacents(subject, paItem);
        assertOutgoingAdjacents(changedSubject, paItem);
    }

    @Test
    public void testThatChangingTypeOfItemTwiceDoesntChangeEquivalence() throws WriteException {
        Item subject = new Item(Id.valueOf(10), Publisher.AMAZON_UK);
        subject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(subject, paItem);
        Episode changedSubject = new Episode(Id.valueOf(10), Publisher.AMAZON_UK);
        changedSubject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(changedSubject, paItem);
        Item changedSubject2 = new Item(Id.valueOf(10), Publisher.AMAZON_UK);
        changedSubject2.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(changedSubject2, paItem);

        assertIncomingAdjacent(paItem, subject);
        assertIncomingAdjacent(paItem, changedSubject);
        assertIncomingAdjacent(paItem, changedSubject2);
    }

    @Test
    public void testAddingAnEquivalentResourceWithChangedType() throws WriteException {
        Item subject = new Item(Id.valueOf(10), Publisher.AMAZON_UK);
        subject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(subject, paItem);
        Episode changedSubject = new Episode(Id.valueOf(10), Publisher.AMAZON_UK);
        changedSubject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(changedSubject, paItem);

        assertOutgoingAdjacents(subject, paItem);
        assertIncomingAdjacent(paItem, subject);
        assertOutgoingAdjacents(changedSubject, paItem);
        assertIncomingAdjacent(paItem, changedSubject);


        makeEquivalent(subject, paItem, c4Item);
        makeEquivalent(changedSubject, paItem);

        assertOutgoingAdjacents(subject, paItem, c4Item);
        assertIncomingAdjacent(paItem, subject);
        assertIncomingAdjacent(c4Item, subject);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
        assertOutgoingAdjacents(changedSubject, paItem, c4Item);
        assertIncomingAdjacent(paItem, changedSubject);
        assertIncomingAdjacent(c4Item, changedSubject);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
    }

    @Test
    public void testMakingExistingEquivalenceToEquivalateToItemWithChangedType() throws WriteException {
        Item subject = new Item(Id.valueOf(10), Publisher.AMAZON_UK);
        subject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(paItem, subject);
        Episode changedSubject = new Episode(Id.valueOf(10), Publisher.AMAZON_UK);
        changedSubject.setThisOrChildLastUpdated(new DateTime(DateTimeZone.UTC));
        makeEquivalent(paItem, changedSubject);

        assertIncomingAdjacent(subject, paItem);
        assertIncomingAdjacent(changedSubject, paItem);
    }

    @Test
    public void testAddingAFourthEquivalentResource() throws WriteException {

        makeEquivalent(bbcItem, paItem, c4Item);

        assertOutgoingAdjacents(bbcItem, paItem, c4Item);
        assertIncomingAdjacent(paItem, bbcItem);
        assertIncomingAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
        
        /*    BBC ----> PA
         *     |        ^
         *     |   ____/
         *     V _/
         *    C4  ----> ITV
         */
        makeEquivalent(c4Item, paItem, itvItem);

        assertOutgoingAdjacents(bbcItem, paItem, c4Item);
        assertOutgoingAdjacents(c4Item, paItem, itvItem);
        assertIncomingAdjacent(paItem, bbcItem);
        assertIncomingAdjacent(paItem, c4Item);
        assertIncomingAdjacent(c4Item, bbcItem);
        assertIncomingAdjacent(itvItem, c4Item);
        assertOnlyTransitivelyEquivalent(bbcItem, itvItem);
        assertOnlyTransitivelyEquivalent(paItem, itvItem);
    }

    @Test
    public void testJoiningTwoPairsOfEquivalents() throws WriteException {
        makeEquivalent(paItem, c4Item);
        makeEquivalent(itvItem, fiveItem);

        assertOutgoingAdjacents(paItem, c4Item);
        assertIncomingAdjacent(c4Item, paItem);
        assertOutgoingAdjacents(itvItem, fiveItem);
        assertIncomingAdjacent(fiveItem, itvItem);

        makeEquivalent(bbcItem, paItem, itvItem);

        assertEquals(
                ImmutableSet.copyOf(Lists.transform(
                        ImmutableList.of(bbcItem, paItem, itvItem, c4Item, fiveItem),
                        Identifiables.toId()
                )),
                graphOf(bbcItem).getEquivalenceSet()
        );

        makeEquivalent(bbcItem, Publisher.all());

        assertEquals(ImmutableSet.of(bbcItem.getId()), graphOf(bbcItem).getEquivalenceSet());
    }

    @Test
    public void testUpdatesForIdInItsOwnGraph() throws WriteException {

        Optional<EquivalenceGraphUpdate> possibleUpdate = makeEquivalent(bbcItem, paItem, itvItem);

        checkUpdate(
                possibleUpdate,
                bbcItem,
                ImmutableSet.<EquivalenceGraph>of(),
                ImmutableSet.of(paItem.getId(), itvItem.getId())
        );

        possibleUpdate = makeEquivalent(bbcItem, Publisher.all(), itvItem, c4Item);

        checkUpdate(
                possibleUpdate,
                bbcItem,
                ImmutableSet.of(graphOf(paItem)),
                ImmutableSet.of(c4Item.getId())
        );

        possibleUpdate = makeEquivalent(bbcItem, Publisher.all());

        checkUpdate(
                possibleUpdate,
                bbcItem,
                ImmutableSet.of(graphOf(itvItem), graphOf(c4Item)),
                ImmutableSet.<Id>of()
        );
    }

    @Test
    public void testUpdatesForIdInAnotherGraph() throws WriteException {

        Optional<EquivalenceGraphUpdate> possibleUpdate = makeEquivalent(paItem, c4Item);

        checkUpdate(
                possibleUpdate,
                paItem,
                ImmutableSet.<EquivalenceGraph>of(),
                ImmutableSet.of(c4Item.getId())
        );

        possibleUpdate = makeEquivalent(bbcItem, paItem, itvItem);

        checkUpdate(
                possibleUpdate,
                bbcItem,
                ImmutableSet.<EquivalenceGraph>of(),
                ImmutableSet.of(paItem.getId(), itvItem.getId())
        );

        possibleUpdate = makeEquivalent(paItem, Publisher.all(), itvItem, fiveItem);

        checkUpdate(
                possibleUpdate,
                bbcItem,
                ImmutableSet.of(graphOf(c4Item)),
                ImmutableSet.of(fiveItem.getId())
        );

        possibleUpdate = makeEquivalent(paItem, Publisher.all(), fiveItem);

        checkUpdate(
                possibleUpdate,
                bbcItem,
                ImmutableSet.<EquivalenceGraph>of(),
                ImmutableSet.<Id>of()
        );
    }

    @Test
    public void testRemovingAnEquivalentResource() throws WriteException {

        makeEquivalent(bbcItem, paItem, c4Item);

        assertOutgoingAdjacents(bbcItem, paItem, c4Item);
        assertIncomingAdjacent(paItem, bbcItem);
        assertIncomingAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);

        makeEquivalent(bbcItem,
                ImmutableSet.of(bbcItem.getSource(), paItem.getSource(), c4Item.getSource()), paItem
        );

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);

        assertThat(graphOf(c4Item), adjacents(
                c4Item.getId(),
                incomingEdges(ImmutableSet.of(c4Item.toRef().getId()))
        ));
        assertThat(graphOf(c4Item), adjacents(
                c4Item.getId(),
                outgoingEdges(ImmutableSet.of(c4Item.toRef().getId()))
        ));

        assertThat(graphOf(bbcItem), adjacencyList(not(hasKey(c4Item.getId()))));
        assertThat(graphOf(paItem), adjacencyList(not(hasKey(c4Item.getId()))));
        assertThat(graphOf(c4Item), adjacencyList(not(hasKey(bbcItem.getId()))));
        assertThat(graphOf(c4Item), adjacencyList(not(hasKey(paItem.getId()))));
    }

    @Test
    public void testDoesntWriteEquivalentsForIgnoredPublishers() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);

        makeEquivalent(paItem, c4Item, paItem, bbcItem);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);
    }

    @Test
    public void testDoesntOverWriteEquivalentsForIgnoredPublishers() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);

        makeEquivalent(paItem, c4Item, paItem, c4Item);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertOutgoingAdjacents(paItem, c4Item);
        assertIncomingAdjacent(paItem, bbcItem);
        assertIncomingAdjacent(c4Item, paItem);
        assertOnlyTransitivelyEquivalent(bbcItem, c4Item);
    }

    @Test
    public void testCanRunTwoWriteSimultaneously() throws InterruptedException, WriteException {

        ExecutorService executor = Executors.newFixedThreadPool(2);

        final Item one = bbcItem;
        final Item two = paItem;
        final Item three = itvItem;
        final Item four = c4Item;
        final Item five = fiveItem;

        makeEquivalent(three, two, four);

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch finish = new CountDownLatch(2);
        executor.submit(() -> {
            start.await();
            makeEquivalent(one, two, one, two);
            finish.countDown();
            return null;
        });
        executor.submit(() -> {
            start.await();
            makeEquivalent(four, five, four, five);
            finish.countDown();
            return null;
        });

        start.countDown();
        assertTrue(finish.await(10, TimeUnit.SECONDS));

        assertOutgoingAdjacents(one, two);
        assertOnlyTransitivelyEquivalent(one, three);
        assertOnlyTransitivelyEquivalent(one, four);
        assertOnlyTransitivelyEquivalent(one, five);

        assertIncomingAdjacent(two, one, three);
        assertOnlyTransitivelyEquivalent(two, four);
        assertOnlyTransitivelyEquivalent(two, five);

        assertOutgoingAdjacents(three, two, four);
        assertOnlyTransitivelyEquivalent(three, one);
        assertOnlyTransitivelyEquivalent(three, five);

        assertIncomingAdjacent(four, three);
        assertOutgoingAdjacents(four, five);
        assertOnlyTransitivelyEquivalent(four, one);
        assertOnlyTransitivelyEquivalent(four, two);

        assertIncomingAdjacent(five, four);
        assertOnlyTransitivelyEquivalent(five, one);
        assertOnlyTransitivelyEquivalent(five, two);
        assertOnlyTransitivelyEquivalent(five, three);
    }

    @Test
    public void testSplittingASetInThree() throws WriteException {
        makeEquivalent(paItem, itvItem);
    }

    @Test
    public void testJoiningAndSplittingTwoLargeSets() throws WriteException {

        for (Integer id : ContiguousSet.create(
                Range.closedOpen(3, 43),
                DiscreteDomain.integers()
        )) {
            DateTime now = new DateTime(DateTimeZones.UTC);
            ItemRef ref = new ItemRef(Id.valueOf(id), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(
                    ref,
                    ImmutableSet.<ResourceRef>of(bbcItem.toRef()),
                    sources(ref, bbcItem)
            );
            ref = new ItemRef(Id.valueOf(id + 200), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(
                    ref,
                    ImmutableSet.<ResourceRef>of(paItem.toRef()),
                    sources(ref, paItem)
            );
        }

        EquivalenceGraph initialBbcGraph = graphOf(bbcItem);
        EquivalenceGraph initialPaGraph = graphOf(paItem);

        assertThat(initialBbcGraph.getAdjacencyList().size(), is(41));
        assertThat(initialPaGraph.getAdjacencyList().size(), is(41));
        assertThat(initialBbcGraph, adjacencyList(not(hasKey(paItem.getId()))));
        assertThat(initialPaGraph, adjacencyList(not(hasKey(bbcItem.getId()))));

        Optional<EquivalenceGraphUpdate> update = makeEquivalent(bbcItem, paItem);
        assertTrue(update.isPresent());
        assertEquals(graphOf(bbcItem), update.get().getUpdated());
        assertEquals(1, update.get().getDeleted().size());

        assertThat(graphOf(bbcItem).getAdjacencyList().size(), is(82));
        assertThat(graphOf(paItem).getAdjacencyList().size(), is(82));
        assertThat(
                graphOf(bbcItem),
                adjacents(bbcItem.getId(), outgoingEdges(hasItem(paItem.toRef().getId())))
        );
        assertThat(graphOf(bbcItem), adjacencyList(hasKey(paItem.getId())));
        assertThat(graphOf(paItem), adjacents(paItem.getId(), incomingEdges(hasItem(bbcItem.toRef().getId()))));
        assertThat(graphOf(paItem), adjacencyList(hasKey(bbcItem.getId())));

        update = makeEquivalent(bbcItem, sources(bbcItem, paItem));
        assertTrue(update.isPresent());
        assertEquals(graphOf(bbcItem), update.get().getUpdated());
        assertEquals(1, update.get().getCreated().size());

        assertThat(graphOf(bbcItem).getAdjacencyList().size(), is(41));
        assertThat(graphOf(paItem).getAdjacencyList().size(), is(41));
        assertThat(graphOf(bbcItem), adjacents(
                bbcItem.getId(),
                outgoingEdges(not(hasItem(paItem.toRef().getId())))
        ));
        assertThat(graphOf(bbcItem), adjacencyList(not(hasKey(paItem.getId()))));
        assertThat(graphOf(paItem), adjacents(
                paItem.getId(),
                incomingEdges(not(hasItem(bbcItem.toRef().getId())))
        ));
        assertThat(graphOf(paItem), adjacencyList(not(hasKey(bbcItem.getId()))));

    }

    @Test
    public void testCanWriteLargeSet() throws WriteException {

        for (Integer id : ContiguousSet.create(
                Range.closedOpen(3, 103),
                DiscreteDomain.integers()
        )) {
            DateTime now = new DateTime(DateTimeZones.UTC);
            ItemRef ref = new ItemRef(Id.valueOf(id), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(
                    ref,
                    ImmutableSet.<ResourceRef>of(bbcItem.toRef()),
                    sources(ref, bbcItem)
            );
            ref = new ItemRef(Id.valueOf(id + 200), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(
                    ref,
                    ImmutableSet.<ResourceRef>of(paItem.toRef()),
                    sources(ref, paItem)
            );
        }

        EquivalenceGraph initialBbcGraph = graphOf(bbcItem);
        EquivalenceGraph initialPaGraph = graphOf(paItem);

        assertThat(initialBbcGraph.getAdjacencyList().size(), is(101));
        assertThat(initialPaGraph.getAdjacencyList().size(), is(101));
        assertThat(initialBbcGraph, adjacencyList(not(hasKey(paItem.getId()))));
        assertThat(initialPaGraph, adjacencyList(not(hasKey(bbcItem.getId()))));

        assertTrue(makeEquivalent(bbcItem, paItem).isPresent());
    }

    @Test
    public void testAlwaysReturnsEquivalenceUpdateEvenWhenEquivalencesDoNotChange()
            throws Exception {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);

        EquivalenceGraph initialBbcGraph = graphOf(bbcItem);
        EquivalenceGraph initialPaGraph = graphOf(paItem);

        assertOutgoingAdjacents(bbcItem, paItem);
        assertIncomingAdjacent(paItem, bbcItem);

        assertTrue(makeEquivalent(bbcItem, paItem).isPresent());

        assertTrue(initialBbcGraph == graphOf(bbcItem));
        assertTrue(initialPaGraph == graphOf(paItem));
    }

    @Test
    public void testOrphanedContentInAdjacentsIsFixed() throws Exception {
        /*
         * itv -> pa
         * bbc -> pa
         */
        makeEquivalent(itvItem, paItem);
        makeEquivalent(bbcItem, paItem);

        // pa is orphaned, i.e. its graph can't be found
        store.cleanGraphAndIndex(paItem.getId());

        store.updateEquivalences(
                bbcItem.toRef(),
                Sets.newHashSet(bbcItem.toRef(), paItem.toRef()),
                Sets.newHashSet(bbcItem.getSource(), paItem.getSource())
        );

        Optional<EquivalenceGraph> paItemEquivalenceGraph = store.resolveIds(Sets.newHashSet(
                paItem.getId())).get().get(paItem.getId());
        assertTrue(paItemEquivalenceGraph.isPresent());
    }

    @Test
    public void testBreakingEquivalenceCreatesNewGraph() throws Exception {
        /*    BBC ----> PA
         *     |        ^
         *     |   ____/
         *     V _/
         *    C4  ----> ITV
         */
        makeEquivalent(bbcItem, paItem, c4Item);
        makeEquivalent(c4Item, paItem, itvItem);

        Optional<EquivalenceGraphUpdate> updateOptional = makeEquivalent(
                c4Item,
                ImmutableSet.of(Publisher.C4, Publisher.ITV),
                paItem
        );

        assertThat(updateOptional.isPresent(), is(true));

        EquivalenceGraphUpdate update = updateOptional.get();

        assertThat(update.getUpdated().getId(), is(bbcItem.getId()));
        assertThat(
                update.getUpdated().getEquivalenceSet(),
                containsInAnyOrder(bbcItem.getId(), paItem.getId(), c4Item.getId())
        );

        assertThat(update.getCreated().size(), is(1));
        EquivalenceGraph createdGraph = Iterables.getOnlyElement(update.getCreated());
        assertThat(createdGraph.getId(), is(itvItem.getId()));
        assertThat(createdGraph.getEquivalenceSet(), containsInAnyOrder(itvItem.getId()));

        assertThat(update.getDeleted().isEmpty(), is(true));
    }

    private void assertOnlyTransitivelyEquivalent(Item left, Item right) {
        EquivalenceGraph lg = graphOf(left);
        Adjacents la = lg.getAdjacents(left.getId());
        assertFalse(la.hasIncomingAdjacent(right.toRef()));
        assertFalse(la.hasOutgoingAdjacent(right.toRef()));
        assertTrue(lg.getEquivalenceSet().contains(right.getId()));
        EquivalenceGraph rg = graphOf(right);
        Adjacents ra = rg.getAdjacents(right.getId());
        assertFalse(ra.hasIncomingAdjacent(left.toRef()));
        assertFalse(ra.hasOutgoingAdjacent(left.toRef()));
        assertTrue(rg.getEquivalenceSet().contains(left.getId()));
    }

    private void assertIncomingAdjacent(Item subj, Item... adjacents) {
        assertThat(graphOf(subj), adjacents(subj.getId(), incomingEdges(hasItem(subj.toRef().getId()))));
        assertThat(graphOf(subj), adjacents(subj.getId(), outgoingEdges(hasItem(subj.toRef().getId()))));
        for (Item adjacent : adjacents) {
            assertThat(
                    graphOf(subj),
                    adjacents(subj.getId(), incomingEdges(hasItem(adjacent.toRef().getId())))
            );
            assertThat(graphOf(subj), adjacencyList(hasEntry(
                    is(adjacent.getId()),
                    outgoingEdges(hasItems(subj.toRef().getId(), adjacent.toRef().getId()))
            )));
        }
    }

    private void assertOutgoingAdjacents(Item subj, Item... adjacents) {
        assertThat(graphOf(subj), adjacents(subj.getId(), incomingEdges(hasItem(subj.toRef().getId()))));
        assertThat(graphOf(subj), adjacents(subj.getId(), outgoingEdges(hasItem(subj.toRef().getId()))));
        for (Item adjacent : adjacents) {
            assertThat(
                    graphOf(subj),
                    adjacents(subj.getId(), outgoingEdges(hasItem(adjacent.toRef().getId())))
            );
            assertThat(graphOf(subj), adjacencyList(hasEntry(
                    is(adjacent.getId()),
                    incomingEdges(hasItems(subj.toRef().getId(), adjacent.toRef().getId()))
            )));
        }
    }

    private Optional<EquivalenceGraphUpdate> makeEquivalent(Item subj, Item... equivs)
            throws WriteException {
        Iterable<Item> items = Iterables.concat(
                ImmutableList.of(subj),
                ImmutableList.copyOf(equivs)
        );
        ImmutableSet<Publisher> sources = FluentIterable.from(items)
                .transform(Sourceds.toPublisher())
                .toSet();
        return makeEquivalent(subj, sources, equivs);
    }

    private Optional<EquivalenceGraphUpdate> makeEquivalent(Item subj, Set<Publisher> sources,
            Item... equivs) throws WriteException {
        ImmutableList<Item> es = ImmutableList.copyOf(equivs);
        return store.updateEquivalences(subj.toRef(),
                ImmutableSet.copyOf(Iterables.transform(es, Item::toRef)), sources
        );
    }

    private <T> T get(ListenableFuture<T> resolveIds) {
        return Futures.getUnchecked(resolveIds);
    }

    private Set<Publisher> sources(Sourced... srcds) {
        return FluentIterable.from(ImmutableList.copyOf(srcds))
                .transform(Sourceds.toPublisher())
                .toSet();
    }

    private void checkUpdate(Optional<EquivalenceGraphUpdate> possibleUpdate, Item updated,
            ImmutableSet<EquivalenceGraph> created, ImmutableSet<Id> deleted) {
        assertTrue(possibleUpdate.isPresent());
        EquivalenceGraphUpdate update = possibleUpdate.get();
        assertThat("updated", update.getUpdated(), is(graphOf(updated)));
        assertThat("created", update.getCreated(), is(created));
        assertThat("deleted", update.getDeleted(), is(deleted));
    }

    private static Matcher<? super Adjacents> incomingEdges(
            Matcher<? super Set<Id>> subMatcher) {
        return new AdjacentsIncomingEdgesMatcher(subMatcher);
    }

    private static Matcher<? super Adjacents> incomingEdges(Set<? extends Id> set) {
        Set<Id> sets = ImmutableSet.copyOf(set);
        return incomingEdges(equalTo(sets));
    }

    private static class AdjacentsIncomingEdgesMatcher
            extends FeatureMatcher<Adjacents, Set<Id>> {

        public AdjacentsIncomingEdgesMatcher(Matcher<? super Set<Id>> subMatcher) {
            super(subMatcher, "with incoming edges", "incoming edges set");
        }

        @Override
        protected Set<Id> featureValueOf(Adjacents actual) {
            return actual.getIncomingEdges().stream().map(ResourceRef::getId).collect(MoreCollectors.toImmutableSet());
        }
    }

    private static Matcher<? super Adjacents> outgoingEdges(
            Matcher<? super Set<Id>> subMatcher) {
        return new AdjacentsOutgoingEdgesMatcher(subMatcher);
    }

    private static Matcher<? super Adjacents> outgoingEdges(Set<? extends Id> set) {
        Set<Id> sets = ImmutableSet.copyOf(set);
        return outgoingEdges(equalTo(sets));
    }

    public static class AdjacentsOutgoingEdgesMatcher
            extends FeatureMatcher<Adjacents, Set<Id>> {

        public AdjacentsOutgoingEdgesMatcher(Matcher<? super Set<Id>> subMatcher) {
            super(subMatcher, "with outgoing edges", "outgoing edges set");
        }

        @Override
        protected Set<Id> featureValueOf(Adjacents actual) {
            return actual.getOutgoingEdges().stream().map(ResourceRef::getId).collect(MoreCollectors.toImmutableSet());
        }
    }

    private static Matcher<? super EquivalenceGraph> adjacents(Id id,
            Matcher<? super Adjacents> subMatcher) {
        return new EquivalenceGraphAdjacentsMatcher(id, subMatcher);
    }

    private static class EquivalenceGraphAdjacentsMatcher
            extends FeatureMatcher<EquivalenceGraph, Adjacents> {

        private Id id;

        public EquivalenceGraphAdjacentsMatcher(Id id, Matcher<? super Adjacents> subMatcher) {
            super(subMatcher, "with adjacents", "adjacents");
            this.id = id;
        }

        @Override
        protected Adjacents featureValueOf(EquivalenceGraph actual) {
            return actual.getAdjacents(id);
        }
    }

    public static Matcher<? super EquivalenceGraph> adjacencyList(
            Matcher<? super Map<Id, Adjacents>> subMatcher) {
        return new EquivalenceGraphAdjacencyListMatcher(subMatcher);
    }

    private static class EquivalenceGraphAdjacencyListMatcher
            extends FeatureMatcher<EquivalenceGraph, Map<Id, Adjacents>> {

        public EquivalenceGraphAdjacencyListMatcher(
                Matcher<? super Map<Id, Adjacents>> subMatcher) {
            super(subMatcher, "with adjacency list", "adjacency list");
        }

        @Override
        protected Map<Id, Adjacents> featureValueOf(EquivalenceGraph actual) {
            return actual.getAdjacencyList();
        }
    }

    private final class InMemoryEquivalenceGraphStore extends AbstractEquivalenceGraphStore {

        private final Logger log = LoggerFactory.getLogger(getClass());
        private final ConcurrentMap<Id, EquivalenceGraph> store = Maps.newConcurrentMap();
        private final Function<Id, EquivalenceGraph> storeFn = Functions.forMap(store, null);
        private final GroupLock<Id> lock = GroupLock.natural();

        public InMemoryEquivalenceGraphStore() {
            super(new MessageSender<EquivalenceGraphUpdateMessage>() {

                @Override
                public void sendMessage(EquivalenceGraphUpdateMessage message) {
                    // no-op
                }

                @Override
                public void sendMessage(EquivalenceGraphUpdateMessage message,
                        byte[] partitionKey)
                        throws MessagingException {

                }

                @Override
                public void close() throws Exception {
                    // no-op
                }
            });
        }

        @Override
        public ListenableFuture<OptionalMap<Id, EquivalenceGraph>> resolveIds(Iterable<Id> ids) {
            ImmutableMap.Builder<Id, EquivalenceGraph> result = ImmutableMap.builder();
            for (Id id : ids) {
                EquivalenceGraph graph = storeFn.apply(id);
                if (graph != null) {
                    result.put(id, graph);
                }
            }
            OptionalMap<Id, EquivalenceGraph> optionalMap = ImmutableOptionalMap.fromMap(result.build());
            return Futures.immediateFuture(optionalMap);
        }

        @Override
        protected void doStore(ImmutableSet<EquivalenceGraph> graphs) {
            for (EquivalenceGraph graph : graphs) {
                for (Id id : graph.getEquivalenceSet()) {
                    store.put(id, graph);
                }
            }
        }

        @Override
        protected Logger log() {
            return log;
        }

        @Override
        protected GroupLock<Id> lock() {
            return lock;
        }

        protected void cleanGraphAndIndex(Id subjectId) {
            store.remove(subjectId);
        }
    }
}
