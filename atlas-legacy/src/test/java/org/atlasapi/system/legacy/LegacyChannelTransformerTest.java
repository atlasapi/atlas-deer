package org.atlasapi.system.legacy;

import java.util.Set;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class LegacyChannelTransformerTest {

    private LegacyChannelTransformer objectUnderTest = new LegacyChannelTransformer();

    @Test
    public void testApply() throws Exception {
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
    }
}