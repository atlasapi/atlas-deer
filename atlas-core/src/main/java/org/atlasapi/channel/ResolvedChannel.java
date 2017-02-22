package org.atlasapi.channel;

import java.util.List;

import com.google.common.base.Optional;
import org.atlasapi.content.ChannelVariantRef;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannel {

    private final Channel channel;
    private final Optional<List<ChannelGroupSummary>> channelGroupSummaries;
    private final Optional<Channel> parentChannel;
    private final Optional<Iterable<Channel>> channelVariations;
    private final java.util.Optional<List<ChannelVariantRef>> includedVariants;
    private final java.util.Optional<List<ChannelVariantRef>> excludedVariants;
    private Optional<ChannelGroupMembership> channelGroupMembership;

    private ResolvedChannel(
            Channel channel,
            Optional<List<ChannelGroupSummary>> channelGroupSummaries,
            Optional<Channel> parentChannel,
            Optional<Iterable<Channel>> channelVariations,
            Optional<ChannelGroupMembership> channelGroupMembership,
            java.util.Optional<List<ChannelVariantRef>> includedVariants,
            java.util.Optional<List<ChannelVariantRef>> excludedVariants
    ) {
        this.channel = checkNotNull(channel);
        this.channelGroupSummaries = checkNotNull(channelGroupSummaries);
        this.parentChannel = checkNotNull(parentChannel);
        this.channelVariations = checkNotNull(channelVariations);
        this.channelGroupMembership = checkNotNull(channelGroupMembership);
        this.includedVariants = checkNotNull(includedVariants);
        this.excludedVariants = checkNotNull(excludedVariants);
    }

    public static Builder builder(Channel channel) {
        return new Builder(channel);
    }

    public Channel getChannel() {
        return channel;
    }

    public Optional<List<ChannelGroupSummary>> getChannelGroupSummaries() {
        return channelGroupSummaries;
    }

    public Optional<Channel> getParentChannel() {
        return parentChannel;
    }

    public Optional<Iterable<Channel>> getChannelVariations() {
        return channelVariations;
    }

    public Optional<ChannelGroupMembership> getChannelGroupMembership() {
        return channelGroupMembership;
    }

    public java.util.Optional<List<ChannelVariantRef>> getIncludedVariants() {
        return includedVariants;
    }

    public java.util.Optional<List<ChannelVariantRef>> getExcludedVariants() {
        return excludedVariants;
    }

    public static class Builder {

        private final Channel channel;
        private Optional<List<ChannelGroupSummary>> channelGroupSummaries = Optional.absent();
        private Optional<Channel> parentChannel = Optional.absent();
        private Optional<Iterable<Channel>> channelVariations = Optional.absent();
        private Optional<ChannelGroupMembership> channelGroupMembership = Optional.absent();
        private java.util.Optional<List<ChannelVariantRef>> includedVariants = java.util.Optional.empty();
        private java.util.Optional<List<ChannelVariantRef>> excludedVariants = java.util.Optional.empty();

        private Builder(Channel channel) {
            this.channel = channel;
        }

        public Builder withChannelGroupSummaries(Optional<List<ChannelGroupSummary>> channelGroupSummaries) {
            this.channelGroupSummaries = channelGroupSummaries;
            return this;
        }

        public Builder withParentChannel(Optional<Channel> parentChannel) {
            this.parentChannel = parentChannel;
            return this;
        }

        public Builder withChannelVariations(Optional<Iterable<Channel>> channelVariations) {
            this.channelVariations = channelVariations;
            return this;
        }

        public Builder withChannelGroupMembership(Optional<ChannelGroupMembership> channelGroupMembership) {
            this.channelGroupMembership = channelGroupMembership;
            return this;
        }

        public Builder withIncludedVariants(java.util.Optional<List<ChannelVariantRef>> includedVariants) {
            this.includedVariants = includedVariants;
            return this;
        }

        public Builder withExcludedVariants(java.util.Optional<List<ChannelVariantRef>> excludedVariants) {
            this.excludedVariants = excludedVariants;
            return this;
        }

        public ResolvedChannel build() {
            return new ResolvedChannel(
                    channel,
                    channelGroupSummaries,
                    parentChannel,
                    channelVariations,
                    channelGroupMembership,
                    includedVariants,
                    excludedVariants
            );
        }

        public static Builder copyOf(ResolvedChannel resolvedChannel) {
            return new Builder(resolvedChannel.getChannel())
                    .withParentChannel(resolvedChannel.getParentChannel())
                    .withChannelVariations(resolvedChannel.getChannelVariations())
                    .withChannelGroupSummaries(resolvedChannel.getChannelGroupSummaries())
                    .withChannelGroupMembership(resolvedChannel.getChannelGroupMembership())
                    .withIncludedVariants(resolvedChannel.getIncludedVariants())
                    .withExcludedVariants(resolvedChannel.getExcludedVariants());
        }
    }

}
