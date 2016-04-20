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

public class ContentGraphServiceSelectorTest {
    
    private ContentGraphServiceSelector serviceSelector;

    private IndexQueryParams indexQueryParams;
    private Set<Publisher> publishers;

    @Before
    public void setUp() throws Exception {
        serviceSelector = ContentGraphServiceSelector.create();

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
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("key", "")
                .put("actionableFilterParameters", "")
                .put("brand.id", "")
                .put("region", "")
                .put("type", "series")
                .put("order_by", "")
                .put("annotations", "")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }

    @Test
    public void supportsActionableEpisodesUnderSeriesQuery() throws Exception {
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("key", "")
                .put("actionableFilterParameters", "")
                .put("series.id", "")
                .put("region", "")
                .put("type", "episode")
                .put("order_by", "")
                .put("annotations", "")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(true)
        );
    }

    @Test
    public void doesNotSupportNonEpisodeType() throws Exception {
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("key", "")
                .put("actionableFilterParameters", "")
                .put("series.id", "")
                .put("region", "")
                .put("type", "item")
                .put("order_by", "")
                .put("annotations", "")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }

    @Test
    public void supportsOptionalParameters() throws Exception {
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("key", "")
                .put("actionableFilterParameters", "")
                .put("series.id", "")
                .put("region", "")
                .put("type", "episode")
                .put("order_by", "")
                .put("annotations", "")
                .put("limit", "")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(true)
        );
    }

    @Test
    public void supportsOptionalParametersMissing() throws Exception {
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("actionableFilterParameters", "")
                .put("series.id", "")
                .put("type", "episode")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(true)
        );
    }

    @Test
    public void doesNotSupportMultipleMutuallyExclusiveParameters() throws Exception {
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("actionableFilterParameters", "")
                .put("series.id", "")
                .put("brand.id", "")
                .put("region", "")
                .put("type", "episode")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }

    @Test
    public void doesNotSupportMissingMutuallyExclusiveParameters() throws Exception {
        ImmutableMap<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("actionableFilterParameters", "")
                .put("region", "")
                .put("type", "episode")
                .build();

        assertThat(
                serviceSelector.getGraphQuery(indexQueryParams, publishers, parameters).isPresent(),
                is(false)
        );
    }
}
