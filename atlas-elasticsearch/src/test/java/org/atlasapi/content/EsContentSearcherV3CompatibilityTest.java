package org.atlasapi.content;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResults;
import org.atlasapi.util.ElasticSearchHelper;
import org.atlasapi.util.NoOpSecondaryIndex;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.node.Node;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.atlasapi.content.ComplexBroadcastTestDataBuilder.broadcast;
import static org.atlasapi.content.ComplexItemTestDataBuilder.complexItem;
import static org.atlasapi.util.ElasticSearchHelper.refresh;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public class EsContentSearcherV3CompatibilityTest {

    private static final Node esClient = ElasticSearchHelper.testNode();
    private EsUnequivalentContentIndex indexer;
    private EsContentTitleSearcher searcher = new EsContentTitleSearcher(esClient.client());

    @BeforeClass
    public static void before() {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @AfterClass
    public static void after() {
        esClient.close();
    }

    @Before
    public void setUp() throws Exception {
        ElasticSearchHelper.refresh(esClient.client());
        indexer = new EsUnequivalentContentIndex(
                esClient.client(),
                EsSchema.CONTENT_INDEX,
                new NoOpContentResolver(),
                mock(ChannelGroupResolver.class),
                new NoOpSecondaryIndex(),
                60000
        );
        indexer.startAsync().awaitRunning(10, TimeUnit.SECONDS);
        refresh(esClient.client());
    }

    private void indexAndWait(Content... contents) throws Exception {
        int indexed = 0;
        for (Content c : contents) {
            if (c instanceof Container) {
                indexer.index(c);
                indexed++;
            } else if (c instanceof Item) {
                indexer.index(c);
                indexed++;
            }
        }
        refresh(esClient.client());
        if (count() < indexed) {
            Assert.fail("Fewer than " + indexed + " content indexed");
        }
    }

    private long count() throws InterruptedException, ExecutionException {
        return new CountRequestBuilder(esClient.client())
                .setIndices(EsSchema.CONTENT_INDEX)
                .execute().get().getCount();
    }

    @After
    public void tearDown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient.client());
    }

    @Test
    public void testFindingBrandsByTitle() throws Exception {

        Brand dragonsDen = brand("/den", "Dragon's den");
        Item dragonsDenItem = complexItem().withBrand(dragonsDen)
                .withBroadcasts(broadcast().build())
                .build();
        Brand doctorWho = brand("/doctorwho", "Doctor Who");
        Item doctorWhoItem = complexItem().withBrand(doctorWho)
                .withBroadcasts(broadcast().build())
                .build();
        Brand theCityGardener = brand("/garden", "The City Gardener");
        Item theCityGardenerItem = complexItem().withBrand(theCityGardener)
                .withBroadcasts(broadcast().build())
                .build();
        Brand eastendersWeddings = brand("/eastenders-weddings", "Eastenders Weddings");
        Item eastendersWeddingsItem = complexItem().withBrand(eastendersWeddings)
                .withBroadcasts(broadcast().build())
                .build();
        Brand eastenders = brand("/eastenders", "Eastenders");
        Item eastendersItem = complexItem().withBrand(eastenders)
                .withBroadcasts(broadcast().build())
                .build();
        Brand politicsEast = brand("/politics", "The Politics Show East");
        Item politicsEastItem = complexItem().withBrand(politicsEast)
                .withBroadcasts(broadcast().build())
                .build();
        Brand meetTheMagoons = brand("/magoons", "Meet the Magoons");
        Item meetTheMagoonsItem = complexItem().withBrand(meetTheMagoons)
                .withBroadcasts(broadcast().build())
                .build();
        Brand theJackDeeShow = brand("/dee", "The Jack Dee Show");
        Item theJackDeeShowItem = complexItem().withBrand(theJackDeeShow)
                .withBroadcasts(broadcast().build())
                .build();
        Brand peepShow = brand("/peep-show", "Peep Show");
        Item peepShowItem = complexItem().withBrand(peepShow)
                .withBroadcasts(broadcast().build())
                .build();
        Brand euromillionsDraw = brand("/draw", "EuroMillions Draw");
        Item euromillionsDrawItem = complexItem().withBrand(euromillionsDraw)
                .withBroadcasts(broadcast().build())
                .build();
        Brand haveIGotNewsForYou = brand("/news", "Have I Got News For You");
        Item haveIGotNewsForYouItem = complexItem().withBrand(haveIGotNewsForYou)
                .withBroadcasts(broadcast().build())
                .build();
        Brand brasseye = brand("/eye", "Brass Eye");
        Item brasseyeItem = complexItem().withBrand(brasseye)
                .withBroadcasts(broadcast().build())
                .build();
        Brand science = brand("/science", "The Story of Science: Power, Proof and Passion");
        Item scienceItem = complexItem().withBrand(science)
                .withBroadcasts(broadcast().build())
                .build();
        Brand theApprentice = brand("/apprentice", "The Apprentice");
        Item theApprenticeItem = complexItem().withBrand(theApprentice)
                .withBroadcasts(broadcast().build())
                .build();

        Item apparent = complexItem().withTitle("Without Apparent Motive")
                .withUri("/item/apparent")
                .withBroadcasts(broadcast().build())
                .build();

        Item englishForCats = complexItem().withUri("/items/cats")
                .withTitle("English for cats")
                .withBroadcasts(broadcast().build())
                .build();

        Item spookyTheCat = complexItem().withTitle("Spooky the Cat")
                .withUri("/item/spookythecat")
                .withBroadcasts(broadcast().build())
                .build();
        Item spooks = complexItem().withTitle("Spooks").withUri("/item/spooks")
                .withBroadcasts(broadcast().withStartTime(new SystemClock().now()
                        .minus(Duration.standardDays(28))).build()).build();

        Item jamieOliversCookingProgramme = complexItem().withUri("/items/oliver/1")
                .withTitle("Jamie Oliver's cooking programme")
                .withDescription("lots of words that are the same alpha beta")
                .withBroadcasts(broadcast().build())
                .build();
        Item gordonRamsaysCookingProgramme = complexItem().withUri("/items/ramsay/2")
                .withTitle("Gordon Ramsay's cooking show")
                .withDescription("lots of words that are the same alpha beta")
                .withBroadcasts(broadcast().build())
                .build();

        Brand rugby = brand("/rugby", "Rugby");
        Item rugbyItem = complexItem().withBrand(rugby)
                .withBroadcasts(broadcast().withChannel(Id.valueOf(1)).build())
                .build();

        Brand sixNationsRugby = brand("/sixnations", "Six Nations Rugby Union");
        Item sixNationsRugbyItem = complexItem().withBrand(sixNationsRugby)
                .withBroadcasts(broadcast().withChannel(Id.valueOf(2)).build())
                .build();

        Brand hellsKitchen = brand("/hellskitchen", "Hell's Kitchen");
        Item hellsKitchenItem = complexItem().withBrand(hellsKitchen)
                .withBroadcasts(broadcast().build())
                .build();

        Brand hellsKitchenUSA = brand("/hellskitchenusa", "Hell's Kitchen");
        Item hellsKitchenUSAItem = complexItem().withBrand(hellsKitchenUSA)
                .withBroadcasts(broadcast().build())
                .build();

        Item we = complexItem().withTitle("W.E.")
                .withUri("/item/we")
                .withBroadcasts(broadcast().build())
                .build();

        indexAndWait(doctorWho, eastendersWeddings, dragonsDen, theCityGardener,
                eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou,
                euromillionsDraw, brasseye, science, politicsEast, theApprentice, rugby,
                sixNationsRugby, hellsKitchen, hellsKitchenUSA, apparent, englishForCats,
                jamieOliversCookingProgramme, gordonRamsaysCookingProgramme, spooks,
                spookyTheCat, dragonsDenItem, doctorWhoItem, theCityGardenerItem,
                eastendersItem, eastendersWeddingsItem, politicsEastItem, meetTheMagoonsItem,
                theJackDeeShowItem, peepShowItem, euromillionsDrawItem, haveIGotNewsForYouItem,
                brasseyeItem, scienceItem, theApprenticeItem, rugbyItem, sixNationsRugbyItem,
                hellsKitchenItem, hellsKitchenUSAItem, we
        );

        check(searcher.search(title("aprentice")).get(), theApprentice);
        check(searcher.search(currentWeighted("apprent")).get(), theApprentice, apparent);
        check(searcher.search(title("den")).get(), dragonsDen, theJackDeeShow);
        check(searcher.search(title("dragon")).get(), dragonsDen);
        check(searcher.search(title("dragons")).get(), dragonsDen);
        check(searcher.search(title("drag den")).get(), dragonsDen);
        check(searcher.search(title("drag")).get(), dragonsDen, euromillionsDraw);
        check(searcher.search(title("dragon's den")).get(), dragonsDen);
        check(searcher.search(title("eastenders")).get(), eastenders, eastendersWeddings);
        check(searcher.search(title("easteners")).get(), eastenders, eastendersWeddings);
        check(searcher.search(title("eastedners")).get(), eastenders, eastendersWeddings);
        check(searcher.search(title("politics east")).get(), politicsEast);
        check(searcher.search(title("eas")).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(title("east")).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(title("end")).get());
        check(searcher.search(title("peep show")).get(), peepShow);
        check(searcher.search(title("peep s")).get(), peepShow);
        check(searcher.search(title("dee")).get(), theJackDeeShow, dragonsDen);
        check(searcher.search(title("jack show")).get(), theJackDeeShow);
        check(searcher.search(title("the jack dee s")).get(), theJackDeeShow);
        check(searcher.search(title("dee show")).get(), theJackDeeShow);
        check(searcher.search(title("hav i got news")).get(), haveIGotNewsForYou);
        check(searcher.search(title("brasseye")).get(), brasseye);
        check(searcher.search(title("braseye")).get(), brasseye);
        check(searcher.search(title("brassey")).get(), brasseye);
        check(
                searcher.search(title("The Story of Science Power Proof and Passion")).get(),
                science
        );
        check(
                searcher.search(title("The Story of Science: Power, Proof and Passion")).get(),
                science
        );
        check(searcher.search(title("Jamie")).get(), jamieOliversCookingProgramme);
        check(searcher.search(title("Spooks")).get(), spooks, spookyTheCat);
    }

    @Test
    public void testFindingBrandsByTitleAfterUpdate() throws Exception {

        Brand theApprentice = brand("/apprentice", "The Apprentice");

        Item theApprenticeItem = complexItem()
                .withBroadcasts(broadcast().build())
                .build();

        Item apparent = complexItem()
                .withBrand(theApprentice)
                .withTitle("Without Apparent Motive")
                .withUri("/item/apparent")
                .withBroadcasts(broadcast().build())
                .build();

        indexAndWait(theApprentice, theApprenticeItem, apparent);

        check(searcher.search(title("aprentice")).get(), theApprentice);

        Brand theApprentice2 = brand(theApprentice.getCanonicalUri(), "Compleletely Different2");
        theApprentice2.setId(theApprentice.getId());
        theApprentice2.setItemRefs(theApprentice.getItemRefs());

        indexAndWait(theApprentice2, theApprenticeItem, apparent);

        checkNot(searcher.search(title("aprentice")).get(), theApprentice);
        check(searcher.search(title("Completely Different2")).get(), theApprentice);
    }

    @Test
    public void testFindingBrandsBySpecialization() throws Exception {

        Brand theApprentice = brand("/apprentice", "The Apprentice");
        Item theApprenticeItem = complexItem().withBrand(theApprentice)
                .withBroadcasts(broadcast().build()).build();

        indexAndWait(theApprentice, theApprenticeItem);

        check(searcher.search(title("aprentice")).get(), theApprentice);

        Item theApprenticeItem2 = new Item();
        Item.copyTo(theApprenticeItem, theApprenticeItem2);
        theApprenticeItem2.setSpecialization(Specialization.RADIO);
        indexer.index(theApprenticeItem2);
        refresh(esClient.client());

        checkNot(
                searcher.search(specializedTitle("aprentice", Specialization.TV)).get(),
                theApprentice
        );
        check(
                searcher.search(specializedTitle("aprentice", Specialization.RADIO)).get(),
                theApprentice
        );
    }

    @Test
    public void testLimitingToPublishers() throws Exception {

        Brand eastenders = brand("/eastenders", "Eastenders");
        Item eastendersItem = complexItem().withBrand(eastenders)
                .withBroadcasts(broadcast().build())
                .build();
        Brand eastendersWeddings = brand("/eastenders-weddings", "Eastenders Weddings");
        Item eastendersWeddingsItem = complexItem().withBrand(eastendersWeddings)
                .withBroadcasts(broadcast().build())
                .build();
        Brand politicsEast = brand("/politics", "The Politics Show East");
        Item politicsEastItem = complexItem().withBrand(politicsEast)
                .withBroadcasts(broadcast().build())
                .build();

        indexAndWait(eastendersWeddings, eastendersWeddingsItem,
                eastenders, eastendersItem,
                politicsEast, politicsEastItem
        );

        check(searcher.search(new SearchQuery(
                "east",
                Selection.ALL,
                ImmutableSet.of(Publisher.BBC),
                1.0f,
                0.0f,
                0.0f
        )).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(new SearchQuery(
                "east",
                Selection.ALL,
                ImmutableSet.of(Publisher.ARCHIVE_ORG),
                1.0f,
                0.0f,
                0.0f
        )).get());

        Brand eastBrand = new Brand("/east", "curie", Publisher.ARCHIVE_ORG);
        eastBrand.setTitle("east");
        eastBrand.setId(Id.valueOf(2517));

        Item eastItem = new Item("/eastItem", "curie", Publisher.ARCHIVE_ORG);
        eastItem.setTitle("east");
        eastItem.setId(Id.valueOf(2518));
        eastItem.setContainerRef(eastBrand.toRef());
        eastItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        eastBrand.setItemRefs(Arrays.asList(eastItem.toRef()));
        indexer.index(eastBrand);
        indexer.index(eastItem);
        refresh(esClient.client());

        check(searcher.search(new SearchQuery(
                "east",
                Selection.ALL,
                ImmutableSet.of(Publisher.ARCHIVE_ORG),
                1.0f,
                0.0f,
                0.0f
        )).get(), eastBrand);
    }

    @Test
    public void testUsesPrefixSearchForShortSearches() throws Exception {
        // commented out for now as order is inverted:
        //check(searcher.search(title("Dr")).get(), doctorWho, dragonsDen);
        check(searcher.search(title("l")).get());
    }

    @Test
    public void testLimitAndOffset() throws Exception {
        Brand eastendersWeddings = brand("/eastenders-weddings", "Eastenders Weddings");
        Item eastendersWeddingsItem = complexItem().withBrand(eastendersWeddings)
                .withBroadcasts(broadcast().build())
                .build();
        Brand eastenders = brand("/eastenders", "Eastenders");
        Item eastendersItem = complexItem().withBrand(eastenders)
                .withBroadcasts(broadcast().build())
                .build();
        Brand politicsEast = brand("/politics", "The Politics Show East");
        Item politicsEastItem = complexItem().withBrand(politicsEast)
                .withBroadcasts(broadcast().build())
                .build();

        indexAndWait(eastendersWeddings, eastendersWeddingsItem,
                eastenders, eastendersItem,
                politicsEast, politicsEastItem
        );

        check(searcher.search(new SearchQuery(
                "eas",
                Selection.ALL,
                Publisher.all(),
                1.0f,
                0.0f,
                0.0f
        )).get(), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(new SearchQuery(
                "eas",
                Selection.limitedTo(2),
                Publisher.all(),
                1.0f,
                0.0f,
                0.0f
        )).get(), eastenders, eastendersWeddings);
        check(searcher.search(new SearchQuery(
                "eas",
                Selection.offsetBy(2),
                Publisher.all(),
                1.0f,
                0.0f,
                0.0f
        )).get(), politicsEast);
    }

    @Test
    public void testBroadcastLocationWeighting() throws Exception {
        Item spookyTheCat = complexItem().withTitle("Spooky the Cat")
                .withUri("/item/spookythecat")
                .withBroadcasts(broadcast().build())
                .build();
        Item spooks = complexItem().withTitle("Spooks").withUri("/item/spooks")
                .withBroadcasts(broadcast().withStartTime(new SystemClock().now()
                        .minus(Duration.standardDays(28))).build()).build();

        indexAndWait(spookyTheCat, spooks);

        check(searcher.search(currentWeighted("spooks")).get(), spooks, spookyTheCat);
        check(searcher.search(title("spook")).get(), spooks, spookyTheCat);
        // commented out for now as order is inverted:
        //check(searcher.search(currentWeighted("spook")).get(), spookyTheCat, spooks);
    }

    @Test
    public void testBrandWithNoChildrenIsNotPickedWithBroadcastWeighting() throws Exception {
        Item spookyTheCat = complexItem().withTitle("Spooky the Cat").withUri("/item/spookythecat")
                .withBroadcasts(
                        broadcast().build()
                ).build();
        Item spooks = complexItem().withTitle("Spooks").withUri("/item/spooks")
                .withBroadcasts(
                        broadcast().withStartTime(
                                new SystemClock().now().minus(Duration.standardDays(28))
                        ).build()
                ).build();

        indexAndWait(spookyTheCat, spooks);

        check(searcher.search(currentWeighted("spook")).get(), spookyTheCat, spooks);

        Brand spookie = new Brand("/spookie2", "curie", Publisher.ARCHIVE_ORG);
        spookie.setTitle("spookie2");
        spookie.setId(Id.valueOf(666));
        indexAndWait(spookie);

        check(searcher.search(currentWeighted("spook")).get(), spookyTheCat, spooks);
    }

    protected static SearchQuery title(String term) {
        return new SearchQuery(term, Selection.ALL, Publisher.all(), 1.0f, 0.0f, 0.0f);
    }

    protected static SearchQuery specializedTitle(String term, Specialization specialization) {
        return SearchQuery.builder(term)
                .withSelection(Selection.offsetBy(0))
                .withSpecializations(Sets.newHashSet(specialization))
                .withPublishers(ImmutableSet.<Publisher>of())
                .withTitleWeighting(1)
                .withBroadcastWeighting(0)
                .withCatchupWeighting(0)
                .withPriorityChannelWeighting(0)
                .build();
    }

    protected static SearchQuery currentWeighted(String term) {
        return new SearchQuery(term, Selection.ALL, Publisher.all(), 1.0f, 0.2f, 0.2f);
    }

    protected static void check(SearchResults result, Identified... content) {
        List<Id> expectedIds = toIds(Arrays.asList(content));

        assertThat(result.getIds().size(), is(expectedIds.size()));
        assertThat(result.getIds().containsAll(expectedIds), is(true));
    }

    protected static void checkNot(SearchResults result, Identified... content) {
        assertFalse(result.getIds().equals(toIds(Arrays.asList(content))));
    }

    private static long id = 100L;

    protected static Brand brand(String uri, String title) {
        Brand b = new Brand(uri, uri, Publisher.BBC);
        b.setTitle(title);
        b.setId(Id.valueOf(id++));
        return b;
    }

    protected static Item item(String uri, String title) {
        return item(uri, title, null);
    }

    protected static Item item(String uri, String title, String description) {
        Item i = new Item();
        i.setId(Id.valueOf(id++));
        i.setTitle(title);
        i.setCanonicalUri(uri);
        i.setDescription(description);
        i.setPublisher(Publisher.BBC);
        i.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        return i;
    }

    protected static Person person(String uri, String title) {
        Person p = new Person(uri, uri, Publisher.BBC);
        p.setTitle(title);
        return p;
    }

    private static List<Id> toIds(List<? extends Identified> content) {
        List<Id> ids = Lists.newArrayList();
        for (Identified description : content) {
            ids.add(description.getId());
        }
        return ids;
    }
}
