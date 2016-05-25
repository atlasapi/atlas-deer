package org.atlasapi.channel;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ChannelTest {

    private String description;
    private String region;
    private Set<String> targetRegions;

    @Before
    public void setUp() throws Exception {
        this.description = "Channel description.";
        this.region = "region";
        this.targetRegions = ImmutableSet.of("region 1", "region 2", "region 3");
    }

    @Test
    public void shortDescriptionIsSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withShortDescription(description)
                .build();

        assertTrue(channel.getShortDescription().equals(description));
    }

    @Test
    public void mediumDescriptionIsSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withMediumDescription(description)
                .build();

        assertTrue(channel.getMediumDescription().equals(description));
    }

    @Test
    public void longDescriptionIsSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withLongDescription(description)
                .build();

        assertTrue(channel.getLongDescription().equals(description));
    }

    @Test
    public void regionIsSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withRegion(region)
                .build();

        assertTrue(channel.getRegion().equals(region));
    }

    @Test
    public void targetRegionsIsSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withTargetRegions(targetRegions)
                .build();

        assertTrue(channel.getTargetRegions().size() == 3);
    }

    @Test
    public void channelTypeIsSetToMasterbrand() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withChannelType(ChannelType.MASTERBRAND)
                .build();

        assertTrue(channel.getChannelType().equals(ChannelType.MASTERBRAND));
    }

    @Test
    public void channelTypeIsSetToChannel() {
        Channel channel = Channel.builder(Publisher.METABROADCAST)
                .withChannelType(ChannelType.CHANNEL)
                .build();

        assertTrue(channel.getChannelType().equals(ChannelType.CHANNEL));
    }

}