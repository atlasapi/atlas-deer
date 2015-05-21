package org.atlasapi.system.legacy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.atlasapi.media.SegmentType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.Segment;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class LegacySegmentTransformerTest {

    private final LegacySegmentTransformer legacySegmentTransformer = new LegacySegmentTransformer();

    org.atlasapi.media.segment.Segment owl = new org.atlasapi.media.segment.Segment();

    @Before
    public void setUp() {
        owl.setPublisher(Publisher.BBC);
        owl.setId(10L);
        owl.setType(SegmentType.VIDEO);
        owl.setDuration(Duration.standardSeconds(10L));
    }

    @Test
    public void transformLegacySegment() throws Exception {
        Segment deer = legacySegmentTransformer.apply(owl);
        assertThat(deer.getId().longValue(), is(owl.getId()));
        assertThat(deer.getSource(), is(owl.getPublisher()));
        assertThat(deer.getType().name().toLowerCase(), is(owl.getType().name().toLowerCase()));
        assertThat(deer.getDuration(), is(owl.getDuration()));
    }
}