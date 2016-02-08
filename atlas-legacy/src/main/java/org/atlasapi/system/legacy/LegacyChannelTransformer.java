package org.atlasapi.system.legacy;

import javax.annotation.Nullable;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class LegacyChannelTransformer
        extends BaseLegacyResourceTransformer<org.atlasapi.media.channel.Channel, Channel> {

    @Nullable
    @Override
    public Channel apply(org.atlasapi.media.channel.Channel input) {
        return Channel.builder(input.getSource())
                .withUri(input.getUri())
                .withId(input.getId())
                .withKey(input.getKey())
                .withHighDefinition(input.getHighDefinition())
                .withTitles(input.getAllTitles())
                .withAdult(input.getAdult())
                .withBroadcaster(input.getBroadcaster())
                .withParent(input.getParent())
                .withVariations(input.getVariations())
                .withStartDate(input.getStartDate())
                .withEndDate(input.getEndDate())
                .withImages(transformImages(input.getAllImages()))
                .withGenres(input.getGenres())
                .withMediaType(MediaType.valueOf(input.getMediaType().toString()))
                .withAvailableFrom(input.getAvailableFrom())
                .withRelatedLinks(transformRelatedLinks(input.getRelatedLinks()))
                .withAliases(transformAliases(input))
                .withChannelGroups(
                        transformChannelNumbers(
                                input.getChannelNumbers(),
                                input.getSource()
                        )
                ).withAdvertiseFrom(input.getAdvertiseFrom())
                .build();
    }

    public org.atlasapi.media.channel.Channel toBasicLegacyChannel(Channel input) {
        org.atlasapi.media.channel.Channel legacyChannel = org.atlasapi.media.channel.Channel.builder()
                .withUri(input.getCanonicalUri())
                .withKey(input.getKey())
                .withHighDefinition(input.getHighDefinition())
                .withBroadcaster(input.getBroadcaster())
                .withSource(input.getSource())
                .withAdvertiseFrom(input.getAdvertiseFrom())
                .withMediaType(
                        org.atlasapi.media.entity.MediaType.valueOf(
                                input.getMediaType().toString().toUpperCase()
                        )
                )
                .build();
        if (input.getParent() != null) {
            legacyChannel.setParent(input.getParent().getId().longValue());
        }
        legacyChannel.setId(input.getId().longValue());
        return legacyChannel;

    }

    private Iterable<ChannelGroupMembership> transformChannelNumbers(
            Iterable<ChannelNumbering> channelNumbers,
            final Publisher publisher
    ) {
        return Iterables.transform(
                channelNumbers,
                new Function<ChannelNumbering, ChannelGroupMembership>() {

                    @Override
                    public ChannelGroupMembership apply(ChannelNumbering channelNumbering) {
                        return ChannelGroupMembership.builder(publisher)
                                .withChannelGroupId(channelNumbering.getChannelGroup())
                                .withChannelId(channelNumbering.getChannel())
                                .withChannelNumber(channelNumbering.getChannelNumber())
                                .withStartDate(channelNumbering.getStartDate())
                                .withEndDate(channelNumbering.getEndDate())
                                .build();
                    }
                }
        );

    }

    private Iterable<TemporalField<Image>> transformImages(
            Iterable<TemporalField<org.atlasapi.media.entity.Image>> legacyImages) {
        return Iterables.transform(
                legacyImages,
                new Function<TemporalField<org.atlasapi.media.entity.Image>, TemporalField<Image>>() {

                    @Override
                    public TemporalField<Image> apply(
                            TemporalField<org.atlasapi.media.entity.Image> input) {
                        return new TemporalField<>(
                                transformImage(input.getValue()),
                                input.getStartDate(),
                                input.getEndDate()
                        );
                    }
                }
        );

    }
}
