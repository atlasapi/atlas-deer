package org.atlasapi.system.legacy;

import java.util.Set;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.MediaType;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.Assert.assertEquals;

public class LegacyChannelTransformerTest {

    private LegacyChannelTransformer objectUnderTest = new LegacyChannelTransformer();

    @Test
    public void allChannelFieldsAreSet() throws Exception {
        String uri = "uri";
        Long id = 1L;
        Boolean hightDefinition = true;
        Boolean adult = true;
        Publisher broadcaster = Publisher.BBC;
        Publisher source = Publisher.BBC_KIWI;
        Long parentId = 2L;
        Set<Long> variations = ImmutableSet.of(3L, 4L);
        LocalDate startDate = new LocalDate("2014-01-31");
        LocalDate endDate = new LocalDate("2014-02-01");
        Set<String> genres = ImmutableSet.of("comedy", "drama");
        org.atlasapi.media.entity.MediaType mediaType = org.atlasapi.media.entity.MediaType.VIDEO;
        Set<Publisher> availableFrom = ImmutableSet.of(Publisher.AMAZON_UK, Publisher.C4);
        DateTime advertiseFrom = DateTime.now();
        String shortDescription = "Short description";
        String mediumDescription = "Medium description";
        String longDescription = "Long description";
        String region = "Region";
        Set<String> targetRegions = Sets.newHashSet("Target Region 1", "Target Region 2");
        org.atlasapi.media.channel.Channel legacyChannel = org.atlasapi.media.channel.Channel.builder()
                .withSource(source)
                .withUri(uri)
                .withHighDefinition(hightDefinition)
                .withAdult(adult)
                .withBroadcaster(broadcaster)
                .withParent(parentId)
                .withVariationIds(variations)
                .withStartDate(startDate)
                .withEndDate(endDate)
                .withGenres(genres)
                .withMediaType(mediaType)
                .withAvailableFrom(availableFrom)
                .withAdvertiseFrom(advertiseFrom)
                .withShortDescription(shortDescription)
                .withMediumDescription(mediumDescription)
                .withLongDescription(longDescription)
                .withRegion(region)
                .withTargetRegions(targetRegions)
                .withChannelType(ChannelType.MASTERBRAND)
                .withInteractive(true)
                .build();
        legacyChannel.setId(id);

        Channel transformed = this.objectUnderTest.apply(legacyChannel);

        assertThat(transformed.getCanonicalUri(), is(uri));
        assertThat(transformed.getSource(), is(source));
        assertThat(transformed.getHighDefinition(), is(hightDefinition));
        assertThat(transformed.getBroadcaster(), is(broadcaster));
        assertThat(transformed.getParent().getId().longValue(), is(parentId));
        assertThat(transformed.getStartDate(), is(startDate));
        assertThat(transformed.getEndDate(), is(endDate));
        assertThat(
                transformed.getGenres(),
                containsInAnyOrder("comedy", "drama", Channel.ADULT_GENRE)
        );
        assertThat(transformed.getMediaType(), is(MediaType.valueOf(mediaType.toString())));
        assertThat(transformed.getAvailableFrom(), is(availableFrom));
        assertThat(transformed.getAdvertiseFrom(), is(advertiseFrom));
        assertThat(transformed.getShortDescription(), is(shortDescription));
        assertThat(transformed.getMediumDescription(), is(mediumDescription));
        assertThat(transformed.getLongDescription(), is(longDescription));
        assertThat(transformed.getRegion(), is(region));
        assertThat(transformed.getTargetRegions(), is(targetRegions));
        assertThat(transformed.getChannelType(), is(org.atlasapi.channel.ChannelType.MASTERBRAND));
        assertThat(transformed.getInteractive(), is(true));
    }

    @Test
    public void noChannelFieldsAreSet() throws Exception {
        org.atlasapi.media.channel.Channel legacyChannel =
                org.atlasapi.media.channel.Channel.builder()
                        .withSource(Publisher.METABROADCAST)
                        .withMediaType(org.atlasapi.media.entity.MediaType.VIDEO)
                        .withChannelType(ChannelType.CHANNEL)
                        .build();
        legacyChannel.setId(19999L);

        Channel transformed = this.objectUnderTest.apply(legacyChannel);

        assertThat(transformed.getCanonicalUri(), isEmptyOrNullString());
        assertThat(transformed.getSource(), is(Publisher.METABROADCAST));
        assertEquals(transformed.getHighDefinition(), null);
        assertEquals(transformed.getBroadcaster(), null);
        assertEquals(transformed.getParent(), null);
        assertEquals(transformed.getStartDate(), null);
        assertEquals(transformed.getEndDate(), null);
        assertEquals(transformed.getGenres(), Sets.newHashSet());
        assertEquals(transformed.getMediaType(), MediaType.VIDEO);
        assertEquals(transformed.getAvailableFrom(), Sets.newHashSet());
        assertEquals(transformed.getAdvertiseFrom(), null);
        assertThat(transformed.getShortDescription(), isEmptyOrNullString());
        assertThat(transformed.getMediumDescription(), isEmptyOrNullString());
        assertThat(transformed.getLongDescription(), isEmptyOrNullString());
        assertThat(transformed.getRegion(), isEmptyOrNullString());
        assertEquals(transformed.getTargetRegions(), Sets.newHashSet());
        assertEquals(transformed.getChannelType(), org.atlasapi.channel.ChannelType.CHANNEL);
        assertEquals(transformed.getInteractive(), false);
    }
}