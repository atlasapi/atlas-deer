package org.atlasapi.equivalence;

import static org.testng.AssertJUnit.assertFalse;
import static org.junit.Assert.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageSender;
import org.atlasapi.util.GroupLock;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.time.DateTimeZones;

public class AbstractEquivalenceGraphStoreTest {

    private final Item bbcItem = new Item(Id.valueOf(1), Publisher.BBC);
    private final Item paItem = new Item(Id.valueOf(2), Publisher.PA);
    private final Item itvItem = new Item(Id.valueOf(3), Publisher.ITV);
    private final Item c4Item = new Item(Id.valueOf(4), Publisher.C4);
    private final Item fiveItem = new Item(Id.valueOf(5), Publisher.FIVE);
    
    private final class InMemoryEquivalenceGraphStore extends AbstractEquivalenceGraphStore {
        
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final ConcurrentMap<Id, EquivalenceGraph> store = Maps.newConcurrentMap();
        private final Function<Id, EquivalenceGraph> storeFn = Functions.forMap(store, null);
        private final GroupLock<Id> lock = GroupLock.natural();
        
        public InMemoryEquivalenceGraphStore() {
            super(new MessageSender() {
                @Override
                public void sendMessage(Message message) throws IOException {
                    //no-op
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
    }
    
    private final InMemoryEquivalenceGraphStore store = new InMemoryEquivalenceGraphStore();
    
    @BeforeMethod
    public void setup() {
        bbcItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        paItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        itvItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        c4Item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        fiveItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
    }
    
    @AfterMethod
    public void tearDown() {
        store.store.clear();
    }
    
    @Test
    public void testMakingTwoResourcesEquivalent() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
        
    }

    private EquivalenceGraph graphOf(Item item) {
        return get(store.resolveIds(ImmutableList.of(item.getId()))).get(item.getId()).get();
    }
    
    @Test
    public void testMakingThreeResourcesEquivalent() throws WriteException {
        makeEquivalent(bbcItem, paItem, c4Item);
        
        assertEfferentAdjacents(bbcItem, paItem, c4Item);
        assertAfferentAdjacent(paItem, bbcItem);
        assertAfferentAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
        
    }

    @Test
    public void testAddingAnEquivalentResource() throws WriteException {
        makeEquivalent(bbcItem, paItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
        
        makeEquivalent(bbcItem, paItem, c4Item);
        
        assertEfferentAdjacents(bbcItem, paItem, c4Item);
        assertAfferentAdjacent(paItem, bbcItem);
        assertAfferentAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
    }

    @Test
    public void testAddingAFourthEquivalentResource() throws WriteException {
        
        makeEquivalent(bbcItem, paItem, c4Item);
        
        assertEfferentAdjacents(bbcItem, paItem, c4Item);
        assertAfferentAdjacent(paItem, bbcItem);
        assertAfferentAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
        
        /*    BBC ----> PA
         *     |        ^
         *     |   ____/
         *     V _/
         *    C4  ----> ITV
         */
        makeEquivalent(c4Item, paItem, itvItem);
        
        assertEfferentAdjacents(bbcItem, paItem, c4Item);
        assertEfferentAdjacents(c4Item, paItem, itvItem);
        assertAfferentAdjacent(paItem, bbcItem);
        assertAfferentAdjacent(paItem, c4Item);
        assertAfferentAdjacent(c4Item, bbcItem);
        assertAfferentAdjacent(itvItem, c4Item);
        assertOnlyTransitivelyEquivalent(bbcItem, itvItem);
        assertOnlyTransitivelyEquivalent(paItem, itvItem);
    }
    
    @Test
    public void testJoiningTwoPairsOfEquivalents() throws WriteException {
        makeEquivalent(paItem, c4Item);
        makeEquivalent(itvItem, fiveItem);
        
        assertEfferentAdjacents(paItem, c4Item);
        assertAfferentAdjacent(c4Item, paItem);
        assertEfferentAdjacents(itvItem, fiveItem);
        assertAfferentAdjacent(fiveItem, itvItem);
        
        makeEquivalent(bbcItem, paItem, itvItem);
        
        assertEquals(ImmutableSet.copyOf(Lists.transform(ImmutableList.of(bbcItem, paItem, itvItem, c4Item, fiveItem), Identifiables.toId())), 
                graphOf(bbcItem).getEquivalenceSet());
        
        makeEquivalent(bbcItem, Publisher.all());
        
        assertEquals(ImmutableSet.of(bbcItem.getId()), graphOf(bbcItem).getEquivalenceSet());
        
    }
    
    @Test
    public void testRemovingAnEquivalentResource() throws WriteException {
        
        makeEquivalent(bbcItem, paItem, c4Item);
        
        assertEfferentAdjacents(bbcItem, paItem, c4Item);
        assertAfferentAdjacent(paItem, bbcItem);
        assertAfferentAdjacent(c4Item, bbcItem);
        assertOnlyTransitivelyEquivalent(paItem, c4Item);
        
        makeEquivalent(bbcItem, 
            ImmutableSet.of(bbcItem.getPublisher(), paItem.getPublisher(), c4Item.getPublisher()), paItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
        
        assertThat(graphOf(c4Item), adjacents(c4Item.getId(), afferents(ImmutableSet.of(c4Item.toRef()))));
        assertThat(graphOf(c4Item), adjacents(c4Item.getId(), efferents(ImmutableSet.of(c4Item.toRef()))));
        
        assertThat(graphOf(bbcItem), adjacencyList(not(hasKey(c4Item.getId()))));
        assertThat(graphOf(paItem), adjacencyList(not(hasKey(c4Item.getId()))));
        assertThat(graphOf(c4Item), adjacencyList(not(hasKey(bbcItem.getId()))));
        assertThat(graphOf(c4Item), adjacencyList(not(hasKey(paItem.getId()))));
        
    }
    
    @Test
    public void testDoesntWriteEquivalentsForIgnoredPublishers() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
        
        makeEquivalent(paItem, c4Item, paItem, bbcItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
    }

    @Test
    public void testDoesntOverWriteEquivalentsForIgnoredPublishers() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
        
        makeEquivalent(paItem, c4Item, paItem, c4Item);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertEfferentAdjacents(paItem, c4Item);
        assertAfferentAdjacent(paItem, bbcItem);
        assertAfferentAdjacent(c4Item, paItem);
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
        final CountDownLatch finish= new CountDownLatch(2);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                start.await();
                makeEquivalent(one, two, one, two);
                finish.countDown();
                return null;
            }
        });
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                start.await();
                makeEquivalent(four, five, four, five);
                finish.countDown();
                return null;
            }
        });
        
        start.countDown();
        assertTrue(finish.await(10, TimeUnit.SECONDS));
        
        assertEfferentAdjacents(one, two);
        assertOnlyTransitivelyEquivalent(one, three);
        assertOnlyTransitivelyEquivalent(one, four);
        assertOnlyTransitivelyEquivalent(one, five);
        
        assertAfferentAdjacent(two, one, three);
        assertOnlyTransitivelyEquivalent(two, four);
        assertOnlyTransitivelyEquivalent(two, five);
        
        assertEfferentAdjacents(three, two, four);
        assertOnlyTransitivelyEquivalent(three, one);
        assertOnlyTransitivelyEquivalent(three, five);
        
        assertAfferentAdjacent(four, three);
        assertEfferentAdjacents(four, five);
        assertOnlyTransitivelyEquivalent(four, one);
        assertOnlyTransitivelyEquivalent(four, two);
        
        assertAfferentAdjacent(five, four);
        assertOnlyTransitivelyEquivalent(five, one);
        assertOnlyTransitivelyEquivalent(five, two);
        assertOnlyTransitivelyEquivalent(five, three);
    }
    
    @Test
    public void testSplittingASetInThree() throws WriteException {
        
        makeEquivalent(paItem, itvItem);
        
    }
    
    private Optional<ImmutableSet<EquivalenceGraph>> makeEquivalent(Item subj, Item...equivs) throws WriteException {
        Iterable<Item> items = Iterables.concat(ImmutableList.of(subj), ImmutableList.copyOf(equivs));
        ImmutableSet<Publisher> sources = FluentIterable.from(items)
                .transform(Sourceds.toPublisher())
                .toSet();
        return makeEquivalent(subj, sources, equivs);
    }

    private Optional<ImmutableSet<EquivalenceGraph>> makeEquivalent(Item subj, Set<Publisher> sources, Item...equivs) throws WriteException {
        ImmutableList<Item> es = ImmutableList.copyOf(equivs);
        return store.updateEquivalences(subj.toRef(), ImmutableSet.copyOf(Iterables.transform(es,new Function<Item,ResourceRef>(){
            @Override
            public ResourceRef apply(Item input) {
                return input.toRef();
            }})), sources);
    }
    
    @Test
    public void testJoiningAndSplittingTwoLargeSets() throws WriteException {
        
        for (Integer id : ContiguousSet.create(Range.closedOpen(3, 43), DiscreteDomain.integers())) {
            DateTime now = new DateTime(DateTimeZones.UTC);
            ItemRef ref = new ItemRef(Id.valueOf(id), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(ref, ImmutableSet.<ResourceRef>of(bbcItem.toRef()), sources(ref, bbcItem));
            ref = new ItemRef(Id.valueOf(id+200), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(ref, ImmutableSet.<ResourceRef>of(paItem.toRef()), sources(ref, paItem));
        }
        
        EquivalenceGraph initialBbcGraph = graphOf(bbcItem);
        EquivalenceGraph initialPaGraph = graphOf(paItem);

        assertThat(initialBbcGraph.getAdjacencyList().size(), is(41));
        assertThat(initialPaGraph.getAdjacencyList().size(), is(41));
        assertThat(initialBbcGraph, adjacencyList(not(hasKey(paItem.getId()))));
        assertThat(initialPaGraph, adjacencyList(not(hasKey(bbcItem.getId()))));

        Optional<ImmutableSet<EquivalenceGraph>> update = makeEquivalent(bbcItem, paItem, bbcItem, paItem);
        assertTrue(update.isPresent());
        assertEquals(1, update.get().size());
        
        assertThat(graphOf(bbcItem).getAdjacencyList().size(), is(82));
        assertThat(graphOf(paItem).getAdjacencyList().size(), is(82));
        assertThat(graphOf(bbcItem), adjacents(bbcItem.getId(), efferents(hasItem(paItem.toRef()))));
        assertThat(graphOf(bbcItem), adjacencyList(hasKey(paItem.getId())));
        assertThat(graphOf(paItem), adjacents(paItem.getId(), afferents(hasItem(bbcItem.toRef()))));
        assertThat(graphOf(paItem), adjacencyList(hasKey(bbcItem.getId())));
        
        update = makeEquivalent(bbcItem, sources(bbcItem, paItem));
        assertTrue(update.isPresent());
        assertEquals(2, update.get().size());
        
        assertThat(graphOf(bbcItem).getAdjacencyList().size(), is(41));
        assertThat(graphOf(paItem).getAdjacencyList().size(), is(41));
        assertThat(graphOf(bbcItem), adjacents(bbcItem.getId(), efferents(not(hasItem(paItem.toRef())))));
        assertThat(graphOf(bbcItem), adjacencyList(not(hasKey(paItem.getId()))));
        assertThat(graphOf(paItem), adjacents(paItem.getId(), afferents(not(hasItem(bbcItem.toRef())))));
        assertThat(graphOf(paItem), adjacencyList(not(hasKey(bbcItem.getId()))));
        
    }
    
    @Test
    public void testAbortsWriteWhenSetTooLarge() throws WriteException {
        
        for (Integer id : ContiguousSet.create(Range.closedOpen(3, 103), DiscreteDomain.integers())) {
            DateTime now = new DateTime(DateTimeZones.UTC);
            ItemRef ref = new ItemRef(Id.valueOf(id), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(ref, ImmutableSet.<ResourceRef>of(bbcItem.toRef()), sources(ref, bbcItem));
            ref = new ItemRef(Id.valueOf(id+200), Publisher.YOUVIEW, "", now);
            store.updateEquivalences(ref, ImmutableSet.<ResourceRef>of(paItem.toRef()), sources(ref, paItem));
        }
        
        EquivalenceGraph initialBbcGraph = graphOf(bbcItem);
        EquivalenceGraph initialPaGraph = graphOf(paItem);

        assertThat(initialBbcGraph.getAdjacencyList().size(), is(101));
        assertThat(initialPaGraph.getAdjacencyList().size(), is(101));
        assertThat(initialBbcGraph, adjacencyList(not(hasKey(paItem.getId()))));
        assertThat(initialPaGraph, adjacencyList(not(hasKey(bbcItem.getId()))));

        assertFalse(makeEquivalent(bbcItem, paItem).isPresent());
        
        assertTrue(graphOf(bbcItem) == initialBbcGraph);
        assertTrue(graphOf(paItem) == initialPaGraph);
    }
    
    @Test
    public void testDoesntWriteEquivalencesWhenEquivalentsDontChange() throws WriteException {
        makeEquivalent(bbcItem, paItem, bbcItem, paItem);
        
        EquivalenceGraph initialBbcGraph = graphOf(bbcItem);
        EquivalenceGraph initialPaGraph = graphOf(paItem);
        
        assertEfferentAdjacents(bbcItem, paItem);
        assertAfferentAdjacent(paItem, bbcItem);
        
        assertFalse(makeEquivalent(bbcItem, paItem).isPresent());
        
        assertTrue(initialBbcGraph == graphOf(bbcItem));
        assertTrue(initialPaGraph == graphOf(paItem));
        
    }

//    private void print(Item... items) {
//        OptionalMap<Id, EquivalenceGraph> graphs = allGraphs(items[0], items);
//        for (EquivalenceGraph g : Optional.presentInstances(graphs.values())) {
//            System.out.println(g);
//            System.out.println("---");
//        }
//    }

    private void assertOnlyTransitivelyEquivalent(Item left, Item right) {
        EquivalenceGraph lg = graphOf(left);
        Adjacents la = lg.getAdjacents(left.getId());
        assertFalse(la.hasAfferentAdjacent(right.toRef()));
        assertFalse(la.hasEfferentAdjacent(right.toRef()));
        assertTrue(lg.getEquivalenceSet().contains(right.getId()));
        EquivalenceGraph rg = graphOf(right);
        Adjacents ra = rg.getAdjacents(right.getId());
        assertFalse(ra.hasAfferentAdjacent(left.toRef()));
        assertFalse(ra.hasEfferentAdjacent(left.toRef()));
        assertTrue(rg.getEquivalenceSet().contains(left.getId()));
    }

    private void assertAfferentAdjacent(Item subj, Item...adjacents) {
        assertThat(graphOf(subj), adjacents(subj.getId(), afferents(hasItem(subj.toRef()))));
        assertThat(graphOf(subj), adjacents(subj.getId(), efferents(hasItem(subj.toRef()))));
        for (Item adjacent : adjacents) {
            assertThat(graphOf(subj), adjacents(subj.getId(), afferents(hasItem(adjacent.toRef()))));
            assertThat(graphOf(subj), adjacencyList(hasEntry(
                is(adjacent.getId()),
                efferents(hasItems((ResourceRef)subj.toRef(), adjacent.toRef()))
            )));
        }
    }

    private void assertEfferentAdjacents(Item subj, Item... adjacents) {
        assertThat(graphOf(subj), adjacents(subj.getId(), afferents(hasItem(subj.toRef()))));
        assertThat(graphOf(subj), adjacents(subj.getId(), efferents(hasItem(subj.toRef()))));
        for (Item adjacent : adjacents) {
            assertThat(graphOf(subj), adjacents(subj.getId(), efferents(hasItem(adjacent.toRef()))));
            assertThat(graphOf(subj), adjacencyList(hasEntry(
                is(adjacent.getId()),
                afferents(hasItems((ResourceRef)subj.toRef(), adjacent.toRef()))
            )));
        }
    }

    private <T> T get(ListenableFuture<T> resolveIds) {
        return Futures.getUnchecked(resolveIds);
    }

    private Set<Publisher> sources(Sourced... srcds) {
        return FluentIterable.from(ImmutableList.copyOf(srcds)).transform(Sourceds.toPublisher()).toSet();
    }
    
    public static final Matcher<? super Adjacents> afferents(Matcher<? super Set<ResourceRef>> subMatcher) {
        return new AdjacentsAfferentsMatcher(subMatcher);
    }

    public static final Matcher<? super Adjacents> afferents(Set<? extends ResourceRef> set) {
        Set<ResourceRef> sets = ImmutableSet.<ResourceRef>copyOf(set);
        return afferents(equalTo(sets));
    }
    
    public static class AdjacentsAfferentsMatcher extends FeatureMatcher<Adjacents, Set<ResourceRef>> {

        public AdjacentsAfferentsMatcher(Matcher<? super Set<ResourceRef>> subMatcher) {
            super(subMatcher, "with afferent edges", "afferents set");
        }

        @Override
        protected Set<ResourceRef> featureValueOf(Adjacents actual) {
            return actual.getAfferent();
        }
        
    }

    public static final Matcher<? super Adjacents> efferents(Matcher<? super Set<ResourceRef>> subMatcher) {
        return new AdjacentsEfferentsMatcher(subMatcher);
    }
    
    public static final Matcher<? super Adjacents> efferents(Set<? extends ResourceRef> set) {
        Set<ResourceRef> sets = ImmutableSet.<ResourceRef>copyOf(set);
        return efferents(equalTo(sets));
    }
    
    public static class AdjacentsEfferentsMatcher extends FeatureMatcher<Adjacents, Set<ResourceRef>> {
        
        public AdjacentsEfferentsMatcher(Matcher<? super Set<ResourceRef>> subMatcher) {
            super(subMatcher, "with efferent edges", "efferents set");
        }
        
        @Override
        protected Set<ResourceRef> featureValueOf(Adjacents actual) {
            return actual.getEfferent();
        }
        
    };
    
    public static final Matcher<? super EquivalenceGraph> adjacents(Id id, Matcher<? super Adjacents> subMatcher) {
        return new EquivalenceGraphAdjacentsMatcher(id, subMatcher);
    }
    
    public static class EquivalenceGraphAdjacentsMatcher extends FeatureMatcher<EquivalenceGraph, Adjacents> {

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
    
    public static final Matcher<? super EquivalenceGraph> adjacencyList(Matcher<? super Map<Id, Adjacents>> subMatcher) {
        return new EquivalenceGraphAdjacencyListMatcher(subMatcher);
    }
    
    public static class EquivalenceGraphAdjacencyListMatcher extends FeatureMatcher<EquivalenceGraph, Map<Id, Adjacents>> {

        public EquivalenceGraphAdjacencyListMatcher(Matcher<? super Map<Id, Adjacents>> subMatcher) {
            super(subMatcher, "with adjacency list", "adjacency list");
        }

        @Override
        protected Map<Id, Adjacents> featureValueOf(EquivalenceGraph actual) {
            return actual.getAdjacencyList();
        }
        
    }

}
