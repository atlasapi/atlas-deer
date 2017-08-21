package org.atlasapi.system.legacy;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelType;
import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

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
                .withTimeshifted(input.isTimeshifted())
                .withTitles(input.getAllTitles())
                .withAdult(input.getAdult())
                .withBroadcaster(input.getBroadcaster())
                .withParent(input.getParent())
                .withVariations(input.getVariations())
                .withSameAs(transformSameAs(input.getSameAs()))
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
                .withAdvertiseTo(input.getAdvertiseTo())
                .withShortDescription(input.getShortDescription())
                .withMediumDescription(input.getMediumDescription())
                .withLongDescription(input.getLongDescription())
                .withRegion(input.getRegion())
                .withTargetRegions(input.getTargetRegions())
                .withChannelType(getChannelTypeFromInput(input.getChannelType()))
                .withInteractive(input.getInteractive())
                .build();
    }

    private ChannelType getChannelTypeFromInput(
            @Nullable org.atlasapi.media.channel.ChannelType inputChannelType
    ) {
        if (inputChannelType != null) {
            Optional<ChannelType> type = ChannelType.fromKey(inputChannelType.toKey());
            if (type.isPresent()) {
                return type.get();
            }
        }
        // If the input channel type is null, will set the channel type to be Channel,
        // this is done so that we set the channel type to be Channel by default.
        return ChannelType.CHANNEL;
    }

    public org.atlasapi.media.channel.Channel toBasicLegacyChannel(Channel input) {
        org.atlasapi.media.channel.Channel legacyChannel = org.atlasapi.media.channel.Channel.builder()
                .withUri(input.getCanonicalUri())
                .withKey(input.getKey())
                .withHighDefinition(input.getHighDefinition())
                .withTimeshifted(input.getTimeshifted())
                .withBroadcaster(input.getBroadcaster())
                .withSource(input.getSource())
                .withAdvertiseFrom(input.getAdvertiseFrom())
                .withAdvertiseTo(input.getAdvertiseTo())
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
                channelNumbering -> ChannelGroupMembership.builder(publisher)
                        .withChannelGroupId(channelNumbering.getChannelGroup())
                        .withChannelId(channelNumbering.getChannel())
                        .withChannelNumber(channelNumbering.getChannelNumber())
                        .withStartDate(channelNumbering.getStartDate())
                        .withEndDate(channelNumbering.getEndDate())
                        .build()
        );

    }

    private Iterable<TemporalField<Image>> transformImages(
            Iterable<TemporalField<org.atlasapi.media.entity.Image>> legacyImages) {
        return Iterables.transform(
                legacyImages,
                input -> new TemporalField<>(
                        transformImage(input.getValue()),
                        input.getStartDate(),
                        input.getEndDate()
                )
        );

    }

    private Set<ChannelEquivRef> transformSameAs(Set<org.atlasapi.equiv.ChannelRef> channelRefs) {
        return channelRefs.stream()
                .map(ref -> ChannelEquivRef.create(
                        Id.valueOf(ref.getId()),
                        ref.getUri(),
                        ref.getPublisher()
                ))
                .collect(MoreCollectors.toImmutableSet());
    }
}
