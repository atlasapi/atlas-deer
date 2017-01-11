package org.atlasapi.criteria;

import java.util.List;
import java.util.Queue;

import org.atlasapi.criteria.QueryNode.IntermediateNode;
import org.atlasapi.criteria.QueryNode.TerminalNode;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.attribute.StringAttribute;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Identified;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class AttributeQuerySetTest {

    private final Attribute<String> ZERO =
            StringAttribute.list("id", Identified.class);
    private final Attribute<String> ONE_FIRST =
            StringAttribute.list("one.first", Identified.class);
    private final Attribute<String> ONE_SECOND =
            StringAttribute.list("one.second", Identified.class);
    private final Attribute<String> ONE_TWO_FIRST =
            StringAttribute.list("one.two.first", Identified.class);
    private final Attribute<String> ONE_TWO_SECOND =
            StringAttribute.list("one.two.second", Identified.class);
    private final Attribute<String> ONE_TWO_THREE_FIRST =
            StringAttribute.list("one.two.three.first", Identified.class);
    private final Attribute<String> ONE_TWO_THREE_SECOND =
            StringAttribute.list("one.two.three.second", Identified.class);
    private final Attribute<String> ONE_TWO_THREE_THIRD =
            StringAttribute.list("one.two.three.third", Identified.class);

    @Test
    public void testAddingUncommonPrefix() {
        List<Attribute<String>> attrs = ImmutableList.of(ONE_FIRST, ZERO);
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 3)))),
                terminalNode(allOf(path("id"), queryAttr(is(ZERO)))),
                terminalNode(allOf(path("one", "first"), queryAttr(is(ONE_FIRST))))
        ));
    }

    @Test
    public void testAddingEqualLengthPaths() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST, ONE_TWO_SECOND
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one", "two"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND))))
        ));
    }

    @Test
    public void testAddingLongerEqualLengthPaths() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_THREE_FIRST, ONE_TWO_THREE_SECOND, ONE_TWO_THREE_THIRD
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one", "two", "three"), children(nodes.subList(2, 5)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_THREE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_THREE_SECOND)))),
                terminalNode(allOf(path("third"), queryAttr(is(ONE_TWO_THREE_THIRD))))
        ));
    }

    @Test
    public void testSplittingIntermediate() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_THREE_FIRST, ONE_TWO_THREE_SECOND, ONE_FIRST
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                intermediateNode(allOf(path("two", "three"), children(nodes.subList(4, 6)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_THREE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_THREE_SECOND))))
        ));
    }

    @Test
    public void testSplittingTerminalWithLongerPath() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST, ONE_TWO_THREE_FIRST
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one", "two"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("three", "first"), queryAttr(is(ONE_TWO_THREE_FIRST))))
        ));
    }

    @Test
    public void testSplittingTerminalWithShorterPath() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_THREE_FIRST, ONE_TWO_FIRST
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one", "two"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("three", "first"), queryAttr(is(ONE_TWO_THREE_FIRST))))
        ));
    }

    @Test
    public void testSplittingDeepTerminal() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST, ONE_FIRST, ONE_TWO_SECOND
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                intermediateNode(allOf(path("two"), children(nodes.subList(4, 6)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND))))
        ));
    }

    @Test
    public void testSplittingIntermediateWithShortPath() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST, ONE_TWO_SECOND, ONE_FIRST
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                intermediateNode(allOf(path("two"), children(nodes.subList(4, 6)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND))))
        ));
    }

    @Test
    public void testSplittingDeeperIntermediate() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_THREE_FIRST,
                ONE_TWO_THREE_SECOND,
                ONE_FIRST,
                ONE_TWO_FIRST,
                ONE_TWO_SECOND
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one"), children(nodes.subList(2, 4)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                intermediateNode(allOf(path("two"), children(nodes.subList(4, 7)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND)))),
                intermediateNode(allOf(path("three"), children(nodes.subList(7, 9)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_THREE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_THREE_SECOND))))
        ));
    }

    @Test
    public void testSplittingIntermediatePushesDown() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST,
                ONE_TWO_SECOND,
                ONE_TWO_THREE_FIRST,
                ONE_TWO_THREE_SECOND,
                ONE_SECOND,
                ONE_FIRST
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one"), children(nodes.subList(2, 5)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_SECOND)))),
                intermediateNode(allOf(path("two"), children(nodes.subList(5, 8)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND)))),
                intermediateNode(allOf(path("three"), children(nodes.subList(8, 10)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_THREE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_THREE_SECOND))))
        ));
    }

    @Test
    public void testPushingDownSplitTerminal() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST,
                ONE_TWO_SECOND,
                ONE_TWO_THREE_FIRST,
                ONE_SECOND,
                ONE_TWO_THREE_SECOND,
                ONE_FIRST
        );
        AttributeQuerySet set = new AttributeQuerySet(createQueries(attrs));
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                intermediateNode(allOf(path("one"), children(nodes.subList(2, 5)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_SECOND)))),
                intermediateNode(allOf(path("two"), children(nodes.subList(5, 8)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND)))),
                intermediateNode(allOf(path("three"), children(nodes.subList(8, 10)))),
                terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_THREE_FIRST)))),
                terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_THREE_SECOND))))
        ));
    }

    @Test
    public void testAddingAttributesInAnyOrder() {
        List<Attribute<String>> attrs = ImmutableList.of(
                ONE_TWO_FIRST,
                ONE_TWO_SECOND,
                ONE_TWO_THREE_FIRST,
                ONE_TWO_THREE_SECOND,
                ONE_FIRST,
                ONE_SECOND
        );
        for (List<Attribute<String>> attrPerm : Collections2.permutations(attrs)) {
            AttributeQuerySet set = new AttributeQuerySet(createQueries(attrPerm));
            List<QueryNode> nodes = set.accept(new NodeListingVisitor());

            matchesInOrder(nodes, ImmutableList.of(
                    intermediateNode(allOf(path(), children(nodes.subList(1, 2)))),
                    intermediateNode(allOf(path("one"), children(nodes.subList(2, 5)))),
                    terminalNode(allOf(path("first"), queryAttr(is(ONE_FIRST)))),
                    terminalNode(allOf(path("second"), queryAttr(is(ONE_SECOND)))),
                    intermediateNode(allOf(path("two"), children(nodes.subList(5, 8)))),
                    terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_FIRST)))),
                    terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_SECOND)))),
                    intermediateNode(allOf(path("three"), children(nodes.subList(8, 10)))),
                    terminalNode(allOf(path("first"), queryAttr(is(ONE_TWO_THREE_FIRST)))),
                    terminalNode(allOf(path("second"), queryAttr(is(ONE_TWO_THREE_SECOND))))
            ));
        }
    }

    private List<AttributeQuery<?>> createQueries(List<Attribute<String>> attrs) {
        return Lists.transform(attrs, new Function<Attribute<String>, AttributeQuery<?>>() {

            @Override
            public AttributeQuery<?> apply(Attribute<String> attr) {
                return attr.createQuery(Operators.EQUALS, ImmutableList.of("val"));
            }
        });
    }

    private void matchesInOrder(List<QueryNode> nodes, List<Matcher<QueryNode>> matchers) {
        assertThat("mismatched node and matcher count",
                nodes.size(), is(matchers.size())
        );
        for (int i = 0; i < nodes.size(); i++) {
            QueryNode node = nodes.get(i);
            assertThat(String.format("mismatch at pos %s: ", i), node, matchers.get(i));
        }
    }

    public static final class NodeListingVisitor implements QueryNodeVisitor<List<QueryNode>> {

        List<QueryNode> queries = Lists.newLinkedList();
        Queue<QueryNode> processing = Lists.newLinkedList();

        @Override
        public List<QueryNode> visit(IntermediateNode node) {
            queries.add(node);
            for (QueryNode desc : QueryNode.pathOrdering().sortedCopy(node.getDescendants())) {
                processing.add(desc);
            }
            if (!processing.isEmpty()) {
                processing.poll().accept(this);
            }
            return queries;
        }

        @Override
        public List<QueryNode> visit(TerminalNode node) {
            queries.add(node);
            if (!processing.isEmpty()) {
                processing.poll().accept(this);
            }
            return queries;
        }
    }

    public static final Matcher<QueryNode> intermediateNode(
            final Matcher<? super IntermediateNode> matcher) {
        return new TypeSafeDiagnosingMatcher<QueryNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendDescriptionOf(matcher);
            }

            @Override
            protected boolean matchesSafely(QueryNode item, final Description mismatchDescription) {
                return item.accept(new QueryNodeVisitor<Boolean>() {

                    @Override
                    public Boolean visit(IntermediateNode node) {
                        if (!matcher.matches(node)) {
                            matcher.describeMismatch(node, mismatchDescription);
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public Boolean visit(TerminalNode node) {
                        mismatchDescription.appendText("got terminal node");
                        return false;
                    }

                });
            }
        };
    }

    public static final Matcher<QueryNode> terminalNode(
            final Matcher<? super TerminalNode> matcher) {
        return new TypeSafeDiagnosingMatcher<QueryNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendDescriptionOf(matcher);
            }

            @Override
            protected boolean matchesSafely(QueryNode item, final Description mismatchDescription) {
                return item.accept(new QueryNodeVisitor<Boolean>() {

                    @Override
                    public Boolean visit(TerminalNode node) {
                        if (!matcher.matches(node)) {
                            matcher.describeMismatch(node, mismatchDescription);
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public Boolean visit(IntermediateNode node) {
                        mismatchDescription.appendText("got intermediate node");
                        return false;
                    }

                });
            }
        };
    }

    private <T> Matcher<TerminalNode> queryAttr(Matcher<Attribute<T>> attributeMatcher) {
        return new FeatureMatcher<TerminalNode, Attribute<T>>(attributeMatcher,
                "terminal node with query", "query"
        ) {

            @Override
            @SuppressWarnings("unchecked")
            protected Attribute<T> featureValueOf(TerminalNode actual) {
                return (Attribute<T>) actual.getQuery().getAttribute();
            }
        };
    }

    private Matcher<IntermediateNode> children(List<QueryNode> subList) {
        return new FeatureMatcher<IntermediateNode, List<QueryNode>>(is(subList),
                "intermediate node with descendants", "descendants"
        ) {

            @Override
            protected List<QueryNode> featureValueOf(IntermediateNode actual) {
                return QueryNode.pathOrdering().immutableSortedCopy(actual.getDescendants());
            }
        };
    }

    private Matcher<QueryNode> path(String... segments) {
        List<String> segs = ImmutableList.copyOf(segments);
        return new FeatureMatcher<QueryNode, List<String>>(is(segs),
                "node with path", "path"
        ) {

            @Override
            protected List<String> featureValueOf(QueryNode actual) {
                return actual.pathSegments();
            }
        };
    }

}
