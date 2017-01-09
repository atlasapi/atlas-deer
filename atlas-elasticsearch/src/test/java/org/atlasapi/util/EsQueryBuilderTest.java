package org.atlasapi.util;

import java.util.Set;
import java.util.UUID;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.attribute.BooleanAttribute;
import org.atlasapi.criteria.attribute.DateTimeAttribute;
import org.atlasapi.criteria.attribute.EnumAttribute;
import org.atlasapi.criteria.attribute.FloatAttribute;
import org.atlasapi.criteria.attribute.IdAttribute;
import org.atlasapi.criteria.attribute.IntegerAttribute;
import org.atlasapi.criteria.attribute.StringAttribute;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EsQueryBuilderTest {

    private static final String INDEX = "test";
    private static final String TYPE = "test";

    private enum TestEnum {
        INSTANCE
    }

    private final Attribute<Integer> MINUS_ONE =
            IntegerAttribute.list("minusone", Identified.class);
    private final Attribute<String> ZERO =
            StringAttribute.list("zero", Identified.class);
    private final Attribute<String> ONE_FIRST =
            StringAttribute.list("one.first", Identified.class);
    private final Attribute<String> ONE_SECOND =
            StringAttribute.list("one.second", Identified.class);
    private final Attribute<Float> ONE_TWO_FIRST =
            FloatAttribute.list("one.two.first", Identified.class);
    private final Attribute<TestEnum> ONE_TWO_SECOND =
            EnumAttribute.list(
                    "one.two.second",
                    TestEnum.class,
                    Identified.class
            );
    private final Attribute<Boolean> ONE_TWO_THREE_FIRST =
            BooleanAttribute.list("one.two.three.first", Identified.class);
    private final Attribute<DateTime> ONE_TWO_THREE_SECOND =
            DateTimeAttribute.single("one.two.three.second", Identified.class);
    private final Attribute<Id> ONE_TWO_THREE_THIRD =
            IdAttribute.list("one.two.three.third", Identified.class);

    private final EsQueryBuilder builder = new EsQueryBuilder();

    private static final Node esClient = NodeBuilder.nodeBuilder()
            .local(true).clusterName(UUID.randomUUID().toString())
            .build().start();

    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
                new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }

    @AfterClass
    public static void after() throws Exception {
        esClient.close();
    }

    @After
    public void tearDown() throws Exception {
        ElasticSearchHelper.clearIndices(esClient.client());
        ElasticSearchHelper.refresh(esClient.client());
    }

    @Before
    public void setup() throws Exception {
        createIndex(esClient, INDEX).actionGet();
        putMapping(esClient, INDEX, TYPE,
                Resources.toString(
                        Resources.getResource("es-query-builder-schema.json"),
                        Charsets.UTF_8
                )
        ).actionGet();

        index(
                esClient,
                INDEX,
                TYPE,
                "one",
                Resources.toString(
                        Resources.getResource("es-query-builder-object.json"),
                        Charsets.UTF_8
                )
        ).actionGet();

        ElasticSearchHelper.refresh(esClient.client());
    }

    @Test
    public void testSingleTopLevelQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                createQuery(ZERO, "one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTwoTopLevelQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.of(
                createQuery(ZERO, "one"),
                createQuery(MINUS_ONE, -1)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testSingleNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                createQuery(ONE_FIRST, "one-first-one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTopAndNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.of(
                createQuery(ZERO, "one"),
                createQuery(ONE_FIRST, "one-first-one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTwoNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.of(
                createQuery(ONE_FIRST, "one-first-one"),
                createQuery(ONE_SECOND, "one-second-one")
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testDoublyNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                createQuery(ONE_TWO_FIRST, 1.0f)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTriplyNestedQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                createQuery(ONE_TWO_THREE_FIRST, true)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testTriplyNestedWithTopQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.of(
                createQuery(ZERO, "one"),
                createQuery(ONE_TWO_THREE_FIRST, true)
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testAllTheAttributes() throws Exception {
        ImmutableSet<AttributeQuery<?>> attrQueries = ImmutableSet.of(
                createQuery(ZERO, "one"),
                createQuery(MINUS_ONE, -1),
                createQuery(ONE_FIRST, "one-first-three"),
                createQuery(ONE_SECOND, "one-second-three"),
                createQuery(ONE_TWO_FIRST, 1.0f),
                createQuery(ONE_TWO_SECOND, TestEnum.INSTANCE),
                createQuery(ONE_TWO_THREE_FIRST, true),
                createQuery(ONE_TWO_THREE_SECOND, new DateTime("1987-02-02T14:30:00.000Z")),
                createQuery(ONE_TWO_THREE_THIRD, Id.valueOf(1234))
        );
        for (Set<AttributeQuery<?>> queries : Iterables.skip(Sets.powerSet(attrQueries), 1)) {
            AttributeQuerySet set = new AttributeQuerySet(queries);
            SearchHits hits = queryHits(set);
            assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
        }
    }

    @Test
    public void testPrefixQueryForNonLetterCharacters() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                ONE_FIRST.createQuery(Operators.BEGINNING, ImmutableList.of("#"))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testPrefixQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                ZERO.createQuery(Operators.BEGINNING, ImmutableList.of("on"))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testDateTimeAfterQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                ONE_TWO_THREE_SECOND.createQuery(
                        Operators.AFTER,
                        ImmutableList.of(new DateTime("1986-02-02T14:30:00.000Z"))
                )
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testDateTimeBeforeQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                ONE_TWO_THREE_SECOND.createQuery(
                        Operators.BEFORE,
                        ImmutableList.of(new DateTime("1988-02-02T14:30:00.000Z"))
                )
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testIntegerGreaterThanQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                MINUS_ONE.createQuery(Operators.GREATER_THAN, ImmutableList.of(-2))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    @Test
    public void testIntegerLessThanQuery() throws Exception {
        AttributeQuerySet queries = new AttributeQuerySet(ImmutableList.<AttributeQuery<?>>of(
                MINUS_ONE.createQuery(Operators.LESS_THAN, ImmutableList.of(0))
        ));
        SearchHits hits = queryHits(queries);
        assertThat(Iterables.getOnlyElement(hits).id(), is("one"));
    }

    private <T> AttributeQuery<?> createQuery(Attribute<T> attr, T... vals) {
        return attr.createQuery(Operators.EQUALS, ImmutableList.copyOf(vals));
    }

    private SearchHits queryHits(AttributeQuerySet query) throws Exception {
        return esClient.client().prepareSearch()
                .setQuery(builder.buildQuery(query))
                .execute().get().getHits();
    }

    private ActionFuture<CreateIndexResponse> createIndex(Node client, String index) {
        return client.client().admin().indices().create(
                Requests.createIndexRequest(index)
        );
    }

    private ActionFuture<PutMappingResponse> putMapping(Node client,
            String index, String type, String mapping) {
        return esClient.client().admin().indices().putMapping(
                Requests.putMappingRequest(INDEX).type(TYPE).source(mapping)
        );
    }

    private ActionFuture<IndexResponse> index(Node esClient, String index,
            String type, String id, String object) {
        return esClient.client().index(
                Requests.indexRequest().index(INDEX).type(TYPE).id(id).source(object)
        );
    }

}
