package org.atlasapi.neo4j.spike;

import java.time.Duration;
import java.time.ZonedDateTime;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpikeModuleTest {

    private static final Logger LOG = LoggerFactory.getLogger(SpikeModuleTest.class);

    private SpikeModule spikeModule;

    @Before
    public void setUp() throws Exception {
        spikeModule = SpikeModule.createStandalone();
    }

    @Test
    public void testTestDateCreator() throws Exception {
        ZonedDateTime start = ZonedDateTime.now();
        spikeModule.testDateCreator(100, 1_000_000, ImmutableSet.of(Publisher.BT_TVE_VOD))
                .createTestData();
        ZonedDateTime end = ZonedDateTime.now();

        LOG.info("Finished loading test data. Took: {}", Duration.between(start, end).toString());
    }
}
