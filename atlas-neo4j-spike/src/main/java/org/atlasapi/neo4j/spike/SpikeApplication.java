package org.atlasapi.neo4j.spike;

import java.time.Duration;
import java.time.ZonedDateTime;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;

public class SpikeApplication {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(SpikeApplication.class);

    public static void main(String[] args) throws Exception {
        new SpikeApplication().run();
    }

    private void run() throws Exception {
        SpikeModule spikeModule = SpikeModule.createStandalone();

        ZonedDateTime start = ZonedDateTime.now();
        spikeModule.testDateCreator(100, 100, ImmutableSet.of(Publisher.BT_TVE_VOD))
                .createTestData();
        ZonedDateTime end = ZonedDateTime.now();

        LOG.info("Finished loading test data. Took: {}", Duration.between(start, end).toString());
    }
}
