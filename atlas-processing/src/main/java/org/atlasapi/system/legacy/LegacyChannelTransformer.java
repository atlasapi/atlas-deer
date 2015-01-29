package org.atlasapi.system.legacy;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;

public class LegacyChannelTransformer extends BaseLegacyResourceTransformer<org.atlasapi.media.channel.Channel, Channel> {


    @Nullable
    @Override
    public Channel apply(org.atlasapi.media.channel.Channel input) {
        return Channel.builder(input.getSource())
                .withUri(input.getUri())
                .withHighDefinition(input.getHighDefinition())
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
                .withAliases(transformAliases(input.getAliases()))
                .withChannelGroups(
                        transformChannelNumbers(
                                input.getChannelNumbers(),
                                input.getSource()
                        )
                ).build();
    }

    private Iterable<ChannelGroupMembership> transformChannelNumbers(
            Iterable<ChannelNumbering> channelNumbers,
            final Publisher publisher
    ) {
        return Iterables.transform(channelNumbers, new Function<ChannelNumbering, ChannelGroupMembership>() {
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
        });

    }

    private Iterable<Alias> transformAliases(Iterable<org.atlasapi.media.entity.Alias> aliases) {
        return Iterables.transform(aliases, new Function<org.atlasapi.media.entity.Alias, Alias>() {
            @Override
            public Alias apply( org.atlasapi.media.entity.Alias input) {
                return null;
            }
        });
    }

    private Iterable<RelatedLink> transformRelatedLinks(Iterable<org.atlasapi.media.entity.RelatedLink> legacyRelatedLinks) {
        return Iterables.transform(legacyRelatedLinks, new Function<org.atlasapi.media.entity.RelatedLink, RelatedLink>() {
            @Override
            public RelatedLink apply(org.atlasapi.media.entity.RelatedLink legacyLink) {
                return transformRelatedLink(legacyLink);
            }
        });

    }

    private RelatedLink transformRelatedLink(org.atlasapi.media.entity.RelatedLink legacyLink) {
        return RelatedLink.relatedLink(
                RelatedLink.LinkType.valueOf(legacyLink.getType().toString()),
                    legacyLink.getUrl()
                ).withSourceId(legacyLink.getSourceId())
                .withShortName(legacyLink.getShortName())
                .withTitle(legacyLink.getTitle())
                .withDescription(legacyLink.getDescription())
                .withImage(legacyLink.getImage())
                .withThumbnail(legacyLink.getThumbnail())
                .build();
    }


    private Iterable<TemporalField<Image>> transformImages(Iterable<TemporalField<org.atlasapi.media.entity.Image>> legacyImages) {
        return Iterables.transform(
                legacyImages,
                new Function<TemporalField<org.atlasapi.media.entity.Image>, TemporalField<Image>>() {
                    @Override
                    public TemporalField<Image> apply(TemporalField<org.atlasapi.media.entity.Image> input) {
                        return new TemporalField<>(
                                transformImage(input.getValue()),
                                input.getStartDate(),
                                input.getEndDate()
                        );
                    }
                }
        );

    }
    private Image transformImage(org.atlasapi.media.entity.Image legacyImage) {

        return Image.builder(legacyImage.getCanonicalUri())
                .withHeight(legacyImage.getHeight())
                .withWidth(legacyImage.getWidth())
                .withType(Image.Type.valueOf(legacyImage.getType().toString()))
                .withColor(Image.Color.valueOf(legacyImage.getColor().toString()))
                .withTheme(Image.Theme.valueOf(legacyImage.getTheme().toString()))
                .withAspectRatio(Image.AspectRatio.valueOf(legacyImage.getAspectRatio().toString()))
                .withMimeType(legacyImage.getMimeType())
                .withAvailabilityStart(legacyImage.getAvailabilityStart())
                .withAvailabilityEnd(legacyImage.getAvailabilityEnd())
                .build();

    }
}
