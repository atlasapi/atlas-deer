package org.atlasapi.neo4j.service.resolvers;

import java.util.stream.IntStream;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;

import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EquivalentSetResolverIT extends AbstractNeo4jIT {

    private EquivalentSetResolver equivalentSetResolver;
    private ContentWriter contentWriter;
    private EquivalenceWriter equivalenceWriter;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        equivalentSetResolver = EquivalentSetResolver.create();
        contentWriter = ContentWriter.create(new Timer(), new Timer(), new Timer());
        equivalenceWriter = EquivalenceWriter.create(new Timer());
    }

    @Test
    public void getEquivalentSetForMissingContent() throws Exception {
        ImmutableSet<Id> equivalentSet = equivalentSetResolver.getEquivalentSet(
                Id.valueOf(1L),
                session
        );

        assertThat(equivalentSet.isEmpty(), is(true));
    }

    @Test
    public void getEquivalentSetForContentWithNoGraph() throws Exception {
        Content content = getContent(Id.valueOf(1L), Publisher.METABROADCAST);
        contentWriter.writeContent(content, session);

        ImmutableSet<Id> equivalentSet = equivalentSetResolver.getEquivalentSet(
                content.getId(),
                session
        );

        assertThat(equivalentSet.size(), is(1));
        assertThat(Iterables.getOnlyElement(equivalentSet), is(content.getId()));
    }

    @Test
    public void getEquivalentSetForContentWithGraph() throws Exception {
        ImmutableList<Content> graphContent = IntStream.range(1, 5)
                .mapToObj(id -> getContent(Id.valueOf(id), Iterables.get(Publisher.all(), id)))
                .collect(MoreCollectors.toImmutableList());

        graphContent.forEach(
                content -> contentWriter.writeContent(content, session)
        );

        for (int i = 0; i < graphContent.size() - 1; i++) {
            equivalenceWriter.writeEquivalences(
                    graphContent.get(i).toRef(),
                    ImmutableSet.of(graphContent.get(i + 1).toRef()),
                    Publisher.all(),
                    session
            );
        }

        ImmutableSet<Id> equivalentSet = equivalentSetResolver.getEquivalentSet(
                graphContent.get(1).getId(),
                session
        );

        assertThat(equivalentSet.size(), is(graphContent.size()));
        assertThat(
                equivalentSet.containsAll(
                        graphContent.stream()
                                .map(Content::getId)
                                .collect(MoreCollectors.toImmutableSet())
                ),
                is(true)
        );
    }

    @Test
    public void getEquivalentSetForContentWithGraphGreaterThanMaxDepth() throws Exception {
        ImmutableList<Content> graphContent = IntStream
                .range(1, EquivalentSetResolver.MAX_DEPTH + 3)
                .mapToObj(id -> getContent(Id.valueOf(id), Iterables.get(Publisher.all(), id)))
                .collect(MoreCollectors.toImmutableList());

        // We expect to get the content we search for + MAX_DEPTH additional content
        assertThat(graphContent.size(), is(EquivalentSetResolver.MAX_DEPTH + 2));

        graphContent.forEach(
                content -> contentWriter.writeContent(content, session)
        );

        for (int i = 0; i < graphContent.size() - 1; i++) {
            equivalenceWriter.writeEquivalences(
                    graphContent.get(i).toRef(),
                    ImmutableSet.of(graphContent.get(i + 1).toRef()),
                    Publisher.all(),
                    session
            );
        }

        ImmutableSet<Id> equivalentSet = equivalentSetResolver.getEquivalentSet(
                graphContent.get(0).getId(),
                session
        );

        assertThat(equivalentSet.size(), is(EquivalentSetResolver.MAX_DEPTH + 1));
        assertThat(
                equivalentSet.containsAll(
                        graphContent.stream()
                                .map(Content::getId)
                                .collect(MoreCollectors.toImmutableSet())
                ),
                is(false)
        );
    }

    @Test
    public void getEquivalentSetForGraphWithHighBranchingFactor() throws Exception {
        Content subjectContent = getContent(Id.valueOf(1L), Publisher.METABROADCAST);
        contentWriter.writeContent(subjectContent, session);

        ImmutableList<Content> graphContent = IntStream.range(2, 30)
                .mapToObj(id -> getContent(Id.valueOf(id), Iterables.get(Publisher.all(), id)))
                .collect(MoreCollectors.toImmutableList());

        graphContent.forEach(
                content -> contentWriter.writeContent(content, session)
        );

        equivalenceWriter.writeEquivalences(
                subjectContent.toRef(),
                graphContent.stream()
                        .map(Content::toRef)
                        .collect(MoreCollectors.toImmutableSet()),
                Publisher.all(),
                session
        );

        ImmutableSet<Id> equivalentSet = equivalentSetResolver.getEquivalentSet(
                subjectContent.getId(),
                session
        );

        assertThat(equivalentSet.size(), is(graphContent.size() + 1));
        assertThat(equivalentSet.contains(subjectContent.getId()), is(true));
        assertThat(
                equivalentSet.containsAll(
                        graphContent.stream()
                                .map(Content::getId)
                                .collect(MoreCollectors.toImmutableSet())
                ),
                is(true)
        );
    }

    @Test
    public void getEquivalentSetByQueryingContentWithNoOutgoingEdgesToRestOfGraph()
            throws Exception {
        Content contentWithOutgoingEdge = getContent(Id.valueOf(1L), Publisher.BBC);

        // This content could also have an outgoing edge to itself. That doesn't matter as we
        // want to test whether we can reach the rest of the graph by following edges backwards
        Content contentWithIncomingEdge = getContent(Id.valueOf(2L), Publisher.METABROADCAST);

        contentWriter.writeContent(contentWithOutgoingEdge, session);
        contentWriter.writeContent(contentWithIncomingEdge, session);

        equivalenceWriter.writeEquivalences(
                contentWithOutgoingEdge.toRef(),
                ImmutableSet.of(contentWithIncomingEdge.toRef()),
                Publisher.all(),
                session
        );

        ImmutableSet<Id> equivalentSet = equivalentSetResolver.getEquivalentSet(
                contentWithIncomingEdge.getId(),
                session
        );

        assertThat(equivalentSet.size(), is(2));
        assertThat(
                equivalentSet.containsAll(ImmutableSet.of(
                        contentWithOutgoingEdge.getId(), contentWithIncomingEdge.getId()
                )),
                is(true)
        );
    }

    private Content getContent(Id id, Publisher source) {
        Item item = new Item(id, source);

        item.setThisOrChildLastUpdated(DateTime.now());

        return item;
    }
}
