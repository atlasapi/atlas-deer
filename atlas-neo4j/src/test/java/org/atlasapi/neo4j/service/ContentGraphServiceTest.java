package org.atlasapi.neo4j.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.atlasapi.content.Content;
import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.query.GraphQuery;
import org.atlasapi.neo4j.service.query.QueryExecutor;
import org.atlasapi.neo4j.service.writer.ContentWriter;
import org.atlasapi.neo4j.service.writer.GraphWriter;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentGraphServiceTest {

    @Mock private ContentWriter contentWriter;
    @Mock private GraphWriter graphWriter;
    @Mock private ContentGraphServiceSelector serviceSelector;
    @Mock private QueryExecutor queryExecutor;

    private ContentGraphService contentGraphService;

    private EquivalenceGraph graph;
    private Content content;

    @Before
    public void setUp() throws Exception {
        contentGraphService = new ContentGraphService(
                contentWriter, graphWriter, serviceSelector, queryExecutor
        );

        content = new Item(Id.valueOf(0L), Publisher.METABROADCAST);
        content.setThisOrChildLastUpdated(DateTime.now());
        graph = EquivalenceGraph.valueOf(content.toRef());
    }

    @Test
    public void writeEquivalentSet() throws Exception {
        contentGraphService.writeEquivalentSet(graph, Lists.newArrayList(content));

        verify(contentWriter).write(content);
        verify(graphWriter).writeGraph(graph);
    }

    @Test
    public void supportsQueryParametersDelegatesToServiceSelector() throws Exception {
        IndexQueryParams indexQueryParams = mock(IndexQueryParams.class);
        List<Publisher> publishers = ImmutableList.of(Publisher.METABROADCAST);
        Map<String, String> parameters = ImmutableMap.of();

        GraphQuery graphQuery = mock(GraphQuery.class);

        ListenableFuture<IndexQueryResult> expectedResult = Futures.immediateFuture(
                IndexQueryResult.withSingleId(Id.valueOf(0L))
        );

        when(serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters))
                .thenReturn(Optional.of(graphQuery));
        when(queryExecutor.execute(graphQuery)).thenReturn(expectedResult);

        Optional<ListenableFuture<IndexQueryResult>> result = contentGraphService.query(
                indexQueryParams,
                publishers,
                parameters
        );

        assertThat(result.isPresent(), is(true));

        FluentIterable<Id> actualIds = result.get().get().getIds();
        FluentIterable<Id> expectedIds = expectedResult.get().getIds();

        assertThat(actualIds.size(), is(expectedIds.size()));
        assertThat(actualIds.toSet().containsAll(expectedIds.toSet()), is(true));
    }
}
