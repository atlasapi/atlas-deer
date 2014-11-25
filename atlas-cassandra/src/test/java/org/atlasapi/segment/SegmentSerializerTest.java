package org.atlasapi.segment;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.SegmentProtos;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class SegmentSerializerTest {

    private final SegmentSerializer segmentSerializer = new SegmentSerializer();
    private Segment segment;
    private final DateTime now = DateTime.now(DateTimeZone.UTC);

    @Before
    public void setUp() throws Exception {
        segment = new Segment();
        segment.setId(Id.valueOf(10L));
        segment.setDuration(Duration.standardSeconds(60));
        segment.setType(SegmentType.fromString("video").requireValue());
        segment.setThisOrChildLastUpdated(now);
        segment.setPublisher(Publisher.BBC);
        segment.setLastFetched(now);
        segment.setLastUpdated(now);
        segment.setAliases(ImmutableSet.of(new Alias("testNS", "testV")));
        segment.setTitle("testTitle");
        segment.setRelatedLinks(ImmutableSet.of(
                RelatedLink.relatedLink(RelatedLink.LinkType.ARTICLE, "www.google.com").build()));
        segment.setLongDescription("test");
        segment.setMediumDescription("test");
        segment.setShortDescription("test");
    }

    @Test
    public void testSerialize() throws Exception {
        Segment deserialized = segmentSerializer.deserialize(segmentSerializer.serialize(segment));

        assertThat(deserialized.getId(), is(segment.getId()));
        assertThat(deserialized.getDuration(), is(segment.getDuration()));
        assertThat(deserialized.getType(), is(segment.getType()));
        assertThat(deserialized.getThisOrChildLastUpdated(), is(equalTo(segment.getThisOrChildLastUpdated())));
        assertThat(deserialized.getAliases(), is(segment.getAliases()));
        assertThat(deserialized.getLastFetched(), is(segment.getLastFetched()));
        assertThat(deserialized.getLastUpdated(), is(segment.getLastFetched()));
        assertThat(deserialized.getTitle(), is(segment.getTitle()));
        assertThat(deserialized.getRelatedLinks(), is(segment.getRelatedLinks()));
        assertThat(deserialized.getLongDescription(), is(segment.getLongDescription()));
        assertThat(deserialized.getMediumDescription(), is(segment.getMediumDescription()));
        assertThat(deserialized.getShortDescription(), is(segment.getShortDescription()));
    }
}