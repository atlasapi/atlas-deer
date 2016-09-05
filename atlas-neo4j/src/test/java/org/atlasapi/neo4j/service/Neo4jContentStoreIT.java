package org.atlasapi.neo4j.service;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class Neo4jContentStoreIT extends AbstractNeo4jIT {

    private Neo4jContentStore contentStore;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        contentStore = module.neo4jContentStore(new MetricRegistry());
    }

    @Test
    public void createGraphAndResolveEquivalentSet() throws Exception {
        contentStore.writeEquivalences(
                new ItemRef(Id.valueOf(0L), Publisher.METABROADCAST, "", DateTime.now()),
                ImmutableSet.of(
                        new ItemRef(Id.valueOf(1L), Publisher.BBC, "", DateTime.now()),
                        new ItemRef(Id.valueOf(2L), Publisher.PA, "", DateTime.now())
                ),
                Publisher.all()
        );

        contentStore.getEquivalentSet(Id.valueOf(0L));
    }
}
