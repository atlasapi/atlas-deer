package org.atlasapi.messaging;

import java.util.Map;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.ImmutableOptionalMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceGraphUpdateResolverTest {
    
    @Mock private EquivalenceGraphStore graphStore;
    
    private EquivalenceGraphUpdateResolver graphUpdateResolver;

    private EquivalenceGraph staleUpdated;
    private EquivalenceGraph currentUpdated;
    private EquivalenceGraph staleCreated;
    private EquivalenceGraph currentCreated;

    @Before
    public void setUp() throws Exception {
        graphUpdateResolver = EquivalenceGraphUpdateResolver.create(graphStore);

        staleUpdated = EquivalenceGraph.valueOf(getItem(0L));
        currentUpdated = EquivalenceGraph.valueOf(getItem(1L));

        staleCreated = EquivalenceGraph.valueOf(getItem(2L));
        currentCreated = EquivalenceGraph.valueOf(getItem(3L));
    }

    @Test
    public void resolveUpdatedGraph() throws Exception {
        setupMocks(ImmutableMap.of(staleUpdated.getId(), currentUpdated));

        ImmutableSet<EquivalenceGraph> graphs = graphUpdateResolver.resolve(
                EquivalenceGraphUpdate.builder(staleUpdated).build()
        );

        assertThat(graphs.size(), is(1));
        assertThat(graphs.contains(currentUpdated), is(true));
    }

    @Test
    public void resolveCreatedGraphs() throws Exception {
        setupMocks(ImmutableMap.of(
                staleUpdated.getId(), currentUpdated,
                staleCreated.getId(), currentCreated
        ));

        ImmutableSet<EquivalenceGraph> graphs = graphUpdateResolver.resolve(
                EquivalenceGraphUpdate.builder(staleUpdated)
                        .withCreated(ImmutableList.of(staleCreated))
                        .build()
        );

        assertThat(graphs.size(), is(2));
        assertThat(graphs.contains(currentCreated), is(true));
    }

    @Test
    public void doNotReturnUpdatedGraphIfItDoesNotResolve() throws Exception {
        setupMocks(ImmutableMap.of(), staleUpdated.getId());

        ImmutableSet<EquivalenceGraph> graphs = graphUpdateResolver.resolve(
                EquivalenceGraphUpdate.builder(staleUpdated).build()
        );

        assertThat(graphs.size(), is(0));
    }

    @Test
    public void doNotReturnCreatedGraphIfItDoesNotResolve() throws Exception {
        setupMocks(
                ImmutableMap.of(staleUpdated.getId(), currentUpdated),
                staleCreated.getId()
        );

        ImmutableSet<EquivalenceGraph> graphs = graphUpdateResolver.resolve(
                EquivalenceGraphUpdate.builder(staleUpdated)
                        .withCreated(ImmutableList.of(staleCreated))
                        .build()
        );

        assertThat(graphs.size(), is(1));
        assertThat(graphs.contains(currentUpdated), is(true));
        assertThat(graphs.contains(staleCreated), is(false));
    }

    private void setupMocks(Map<Id, EquivalenceGraph> graphs, Id... missingIds) {
        when(graphStore.resolveIds(
                Sets.union(graphs.keySet(), ImmutableSet.copyOf(missingIds))
        ))
                .thenReturn(Futures.immediateFuture(
                        ImmutableOptionalMap.fromMap(graphs)
                ));
    }

    private ItemRef getItem(long id) {
        return new ItemRef(Id.valueOf(id), Publisher.METABROADCAST, "", DateTime.now());
    }
}
