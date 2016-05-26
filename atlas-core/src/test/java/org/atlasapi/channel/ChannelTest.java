package org.atlasapi.channel;

import java.util.HashSet;
import java.util.Set;

import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ChannelTest {

    private String description = "Channel description.";
    private String region = "region";
    private Set<String> targetRegions = ImmutableSet.of("region 1", "region 2", "region 3");
    private LocalDate startDateValue = LocalDate.parse("2016-05-26");
    private DateTime advertisedFromValue = DateTime.parse("2016-05-27T07:15:30.450Z");

    @Test
    public void noFieldsAreSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .build();

        assertTrue(channel.getTitle() == null);
        assertTrue(channel.getId() == null);
        assertTrue(channel.getMediaType() == null);
        assertTrue(channel.getHighDefinition() == null);
        assertTrue(channel.getRegional() == null);
        assertTrue(channel.getStartDate() == null);
        assertTrue(channel.getAdvertiseFrom() == null);
        assertTrue(channel.getShortDescription() == null);
        assertTrue(channel.getMediumDescription() == null);
        assertTrue(channel.getLongDescription() == null);
        assertTrue(channel.getRegion() == null);
        assertTrue(channel.getTargetRegions().equals(Sets.newHashSet()));
        assertTrue(channel.getChannelType() == null);
        assertTrue(channel.getGenres().equals(Sets.newHashSet()));
        assertTrue(channel.getTargetRegions().equals(Sets.newHashSet()));
        assertTrue(channel.getImages().equals(Sets.newHashSet()));
        assertTrue(channel.getAvailableFrom().equals(Sets.newHashSet()));
        assertTrue(channel.getAliases().equals(Sets.newHashSet()));
        assertTrue(channel.getRelatedLinks().equals(Sets.newHashSet()));
    }

    @Test
    public void allFieldsAreSetToChannel() {
        MediaType mediaType = MediaType.VIDEO;
        HashSet<String> targetRegions = Sets.newHashSet("region 1", "region 2", "region 3");
        Alias alias = new Alias("alias", "value");
        RelatedLink relatedLink = RelatedLink.unknownTypeLink("link").build();
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withTitles(Sets.newHashSet(new TemporalField("title", null, null)))
                .withId(199999L)
                .withMediaType(mediaType)
                .withHighDefinition(true)
                .withRegional(true)
                .withStartDate(startDateValue)
                .withAdvertiseFrom(advertisedFromValue)
                .withShortDescription(description)
                .withMediumDescription(description)
                .withLongDescription(description)
                .withRegion(region)
                .withTargetRegions(this.targetRegions)
                .withChannelType(ChannelType.MASTERBRAND)
                .withGenre("genre")
                .withTargetRegions(targetRegions)
                .withImages(Sets.newHashSet(new TemporalField("image", null, null)))
                .withAvailableFrom(Publisher.INTERNET_VIDEO_ARCHIVE)
                .withAliases(Sets.newHashSet(alias))
                .withRelatedLink(relatedLink)
                .build();



        assertTrue(channel.getTitle().equals("title"));
        assertTrue(channel.getId().equals(Id.valueOf(199999)));
        assertTrue(channel.getMediaType().equals(mediaType));
        assertTrue(channel.getHighDefinition());
        assertTrue(channel.getRegional());
        assertTrue(channel.getStartDate().equals(startDateValue));
        assertTrue(channel.getAdvertiseFrom().equals(advertisedFromValue));
        assertTrue(channel.getShortDescription().equals(description));
        assertTrue(channel.getMediumDescription().equals(description));
        assertTrue(channel.getLongDescription().equals(description));
        assertTrue(channel.getRegion().equals(region));
        assertTrue(channel.getTargetRegions().size() == 3);
        assertTrue(channel.getChannelType().equals(ChannelType.MASTERBRAND));
        assertTrue(channel.getGenres().equals(Sets.newHashSet("genre")));
        assertTrue(channel.getTargetRegions().equals(targetRegions));
        assertTrue(channel.getImages().equals(Sets.newHashSet("image")));
        assertTrue(channel.getAvailableFrom().equals(Sets.newHashSet(Publisher.INTERNET_VIDEO_ARCHIVE)));
        assertTrue(channel.getAliases().equals(Sets.newHashSet(alias)));
        assertTrue(channel.getRelatedLinks().equals(Sets.newHashSet(relatedLink)));
    }

}