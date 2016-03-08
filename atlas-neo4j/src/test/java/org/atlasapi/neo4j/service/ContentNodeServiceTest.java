package org.atlasapi.neo4j.service;

import java.util.Arrays;
import java.util.stream.StreamSupport;

import org.atlasapi.neo4j.Neo4jSessionFactory;
import org.atlasapi.neo4j.model.nodes.ContentNode;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.ogm.session.Session;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ContentNodeServiceTest {

    private ContentNodeService service;

    @Before
    public void setUp() throws Exception {
        Session session = Neo4jSessionFactory.createWithEmbeddedDriver().getNeo4jSession();
        service = ContentNodeService.create(session);
    }

    @Test
    public void testWriteAndFetchSingleContent() throws Exception {
        ContentNode node = ContentNode.builder(1L)
                .withData("data")
                .build();

        ContentNode createdNode = service.createOrUpdate(node, 1);

        assertThat(node.equals(createdNode), is(true));
        assertThat(createdNode.getId(), not(is(nullValue())));

        ContentNode foundNode = service.find(createdNode.getId(), 1);

        assertThat(createdNode, is(foundNode));
        assertThat(createdNode.getId(), is(foundNode.getId()));
    }

    @Test
    public void testGetByContentId() throws Exception {
        ContentNode node = ContentNode.builder(1L).build();

        service.createOrUpdate(node, 0);

        ContentNode foundNode = service.findByContentId(node.getContentId());

        assertThat(node, is(foundNode));
    }

    @Test
    public void testWriteNodeSavesEntireGraph() throws Exception {
        ContentNode nodeD = ContentNode.builder(4L)
                .build();

        ContentNode nodeC = ContentNode.builder(3L)
                .withEquivalents(Lists.newArrayList(nodeD))
                .build();

        ContentNode nodeB = ContentNode.builder(2L)
                .build();

        ContentNode nodeA = ContentNode.builder(1L)
                .withEquivalents(Lists.newArrayList(nodeB, nodeC))
                .build();

        service.createOrUpdate(nodeA, 2);

        ContentNode actualNodeA = service.findByContentId(nodeA.getContentId());
        ContentNode actualNodeB = service.findByContentId(nodeB.getContentId());
        ContentNode actualNodeC = service.findByContentId(nodeC.getContentId());
        ContentNode actualNodeD = service.findByContentId(nodeD.getContentId());

        assertThat(actualNodeA.getContentId(), is(nodeA.getContentId()));
        assertThat(actualNodeB.getContentId(), is(nodeB.getContentId()));
        assertThat(actualNodeC.getContentId(), is(nodeC.getContentId()));
        assertThat(actualNodeD.getContentId(), is(nodeD.getContentId()));

        assertEquivalents(actualNodeA, actualNodeB, actualNodeC);
        assertEquivalents(actualNodeB);
        assertEquivalents(actualNodeC, actualNodeD);
        assertEquivalents(actualNodeD);
    }

    @Test
    public void testWriteGraphUpToLimitedDepth() throws Exception {
        ContentNode nodeD = ContentNode.builder(4L)
                .build();

        ContentNode nodeC = ContentNode.builder(3L)
                .withEquivalents(Lists.newArrayList(nodeD))
                .build();

        ContentNode nodeB = ContentNode.builder(2L)
                .build();

        ContentNode nodeA = ContentNode.builder(1L)
                .withEquivalents(Lists.newArrayList(nodeB, nodeC))
                .build();

        service.createOrUpdate(nodeA, 1);

        ContentNode actualNodeA = service.findByContentId(nodeA.getContentId());
        ContentNode actualNodeB = service.findByContentId(nodeB.getContentId());
        ContentNode actualNodeC = service.findByContentId(nodeC.getContentId());
        ContentNode actualNodeD = service.findByContentId(nodeD.getContentId());

        assertThat(actualNodeA, not(is(nullValue())));
        assertThat(actualNodeB, not(is(nullValue())));
        assertThat(actualNodeC, not(is(nullValue())));
        assertThat(actualNodeD, is(nullValue()));
    }

    @Test
    public void testGetGraphWithCustomQuery() throws Exception {
        ContentNode nodeD = ContentNode.builder(4L)
                .build();

        ContentNode nodeC = ContentNode.builder(3L)
                .withEquivalents(Lists.newArrayList(nodeD))
                .build();

        ContentNode nodeB = ContentNode.builder(2L)
                .withEquivalents(Lists.newArrayList(nodeC))
                .build();

        ContentNode nodeA = ContentNode.builder(1L)
                .withEquivalents(Lists.newArrayList(nodeB))
                .build();

        service.createOrUpdate(nodeA, 3);

        Iterable<ContentNode> equivalentSet = service.getEquivalentSet(2L);

        assertThat(Iterables.size(equivalentSet), is(4));

        ImmutableSet<Long> actualContentIds = StreamSupport
                .stream(equivalentSet.spliterator(), false)
                .map(ContentNode::getContentId)
                .collect(ImmutableCollectors.toSet());

        assertThat(actualContentIds, is(ImmutableSet.of(
                nodeA.getContentId(),
                nodeB.getContentId(),
                nodeC.getContentId(),
                nodeD.getContentId())
        ));
    }

    @Test
    public void testGetGraphIdsWithCustomQuery() throws Exception {
        ContentNode nodeD = ContentNode.builder(Long.MAX_VALUE)
                .build();

        ContentNode nodeC = ContentNode.builder(3L)
                .withEquivalents(Lists.newArrayList(nodeD))
                .build();

        ContentNode nodeB = ContentNode.builder(2L)
                .withEquivalents(Lists.newArrayList(nodeC))
                .build();

        ContentNode nodeA = ContentNode.builder(1L)
                .withEquivalents(Lists.newArrayList(nodeB))
                .build();

        service.createOrUpdate(nodeA, 3);

        ImmutableList<Long> contentIds = service.getEquivalentSetIds(2L);

        assertThat(contentIds.size(), is(3));
        assertThat(
                contentIds.containsAll(ImmutableSet.of(
                    nodeA.getContentId(),
                    nodeC.getContentId(),
                    nodeD.getContentId())),
                is(true));
    }

    @Test
    public void testCustomWrite() throws Exception {
        ContentNode node = ContentNode.builder(0L)
                .withData("data")
                .build();

        service.write(node);

        ContentNode actualNode = service.findByContentId(node.getContentId());

        assertThat(actualNode, is(node));
    }

    private void assertEquivalents(ContentNode node, ContentNode... equivalents) {
        assertThat(node.getEquivalentContent().size(), is(equivalents.length));
        ImmutableSet<Long> expectedContentId = Arrays.stream(equivalents)
                .map(ContentNode::getContentId)
                .collect(ImmutableCollectors.toSet());

        ImmutableSet<Long> actualContentId = node.getEquivalentContent().stream()
                .map(ContentNode::getContentId)
                .collect(ImmutableCollectors.toSet());

        assertThat(actualContentId.size(), is(expectedContentId.size()));
        assertThat(actualContentId.containsAll(expectedContentId), is(true));
    }
}
