package org.atlasapi.neo4j.service;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ContentGraphQueryFactoryTest {
    
    private ContentGraphQueryFactory serviceSelector;

    private IndexQueryParams indexQueryParams;
    private Set<Publisher> publishers;

    @Before
    public void setUp() throws Exception {
        serviceSelector = ContentGraphQueryFactory.create();

        indexQueryParams = new IndexQueryParams(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.of(ImmutableMap.of("location.available", "true")),
                Optional.of(Id.valueOf(0L))
        );
        publishers = Publisher.all();
    }

    @Test
    public void doesNotsupportActionableSeriesUnderBrandQuery() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("key", new String[] { "" })
                .put("actionableFilterParameters", new String[] { "" })
                .put("brand.id", new String[] { "" })
                .put("region", new String[] { "" })
                .put("type", new String[] { "series" })
                .put("order_by", new String[] { "" })
                .put("annotations", new String[] { "" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }

    @Test
    public void supportsActionableEpisodesUnderSeriesQuery() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("key", new String[] { "" })
                .put("actionableFilterParameters", new String[] { "" })
                .put("series.id", new String[] { "" })
                .put("region", new String[] { "" })
                .put("type", new String[] { "episode" })
                .put("order_by", new String[] { "" })
                .put("annotations", new String[] { "" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(true)
        );
    }

    @Test
    public void doesNotSupportNonEpisodeType() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("key", new String[] { "" })
                .put("actionableFilterParameters", new String[] { "" })
                .put("series.id", new String[] { "" })
                .put("region", new String[] { "" })
                .put("type", new String[] { "item" })
                .put("order_by", new String[] { "" })
                .put("annotations", new String[] { "" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }

    @Test
    public void supportsOptionalParameters() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("key", new String[] { "" })
                .put("actionableFilterParameters", new String[] { "" })
                .put("series.id", new String[] { "" })
                .put("region", new String[] { "" })
                .put("type", new String[] { "episode" })
                .put("order_by", new String[] { "" })
                .put("annotations", new String[] { "" })
                .put("limit", new String[] { "" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(true)
        );
    }

    @Test
    public void supportsOptionalParametersMissing() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("actionableFilterParameters", new String[] { "" })
                .put("series.id", new String[] { "" })
                .put("type", new String[] { "episode" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(true)
        );
    }

    @Test
    public void doesNotSupportMultipleMutuallyExclusiveParameters() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("actionableFilterParameters", new String[] { "" })
                .put("series.id", new String[] { "" })
                .put("brand.id", new String[] { "" })
                .put("region", new String[] { "" })
                .put("type", new String[] { "episode" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }

    @Test
    public void doesNotSupportMissingMutuallyExclusiveParameters() throws Exception {
        ImmutableMap<String, String[]> parameters = ImmutableMap.<String, String[]>builder()
                .put("actionableFilterParameters", new String[] { "" })
                .put("region", new String[] { "" })
                .put("type", new String[] { "episode" })
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }
}
