package org.atlasapi.channel;

import java.util.List;
import java.util.Optional;

import org.atlasapi.content.ChannelVariantRef;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannel {

    private final Channel channel;
    private final Optional<List<ChannelGroupSummary>> channelGroupSummaries;
    private final Optional<Channel> parentChannel;
    private final Optional<Iterable<Channel>> channelVariations;
    private final Optional<List<ChannelVariantRef>> includedVariants;
    private final Optional<List<ChannelVariantRef>> excludedVariants;
    private final Optional<ChannelGroupMembership> channelGroupMembership;
    private final Optional<Iterable<Channel>> equivalents;

    private ResolvedChannel(
            Channel channel,
            Optional<List<ChannelGroupSummary>> channelGroupSummaries,
            Optional<Channel> parentChannel,
            Optional<Iterable<Channel>> channelVariations,
            Optional<ChannelGroupMembership> channelGroupMembership,
            Optional<List<ChannelVariantRef>> includedVariants,
            Optional<List<ChannelVariantRef>> excludedVariants,
            Optional<Iterable<Channel>> equivalents
    ) {
        this.channel = checkNotNull(channel);
        this.channelGroupSummaries = checkNotNull(channelGroupSummaries);
        this.parentChannel = checkNotNull(parentChannel);
        this.channelVariations = checkNotNull(channelVariations);
        this.channelGroupMembership = checkNotNull(channelGroupMembership);
        this.includedVariants = checkNotNull(includedVariants);
        this.excludedVariants = checkNotNull(excludedVariants);
        this.equivalents = checkNotNull(equivalents);
    }

    public static Builder builder(Channel channel) {
        return new Builder(channel);
    }

    public static Builder builder() {
        return new Builder();
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

    public Optional<List<ChannelVariantRef>> getIncludedVariants() {
        return includedVariants;
    }

    public Optional<List<ChannelVariantRef>> getExcludedVariants() {
        return excludedVariants;
    }

    public Optional<Iterable<Channel>> getEquivalents() {
        return equivalents;
    }

    public static class Builder {

        private Channel channel;
        private Optional<List<ChannelGroupSummary>> channelGroupSummaries = Optional.empty();
        private Optional<Channel> parentChannel = Optional.empty();
        private Optional<Iterable<Channel>> channelVariations = Optional.empty();
        private Optional<ChannelGroupMembership> channelGroupMembership = Optional.empty();
        private Optional<List<ChannelVariantRef>> includedVariants = Optional.empty();
        private Optional<List<ChannelVariantRef>> excludedVariants = Optional.empty();
        private Optional<Iterable<Channel>> equivalents = Optional.empty();

        private Builder(Channel channel) {
            this.channel = channel;
        }

        private Builder () {}

        public Builder withChannel(Channel channel) {
            this.channel = channel;
            return this;
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

        public Builder withIncludedVariants(Optional<List<ChannelVariantRef>> includedVariants) {
            this.includedVariants = includedVariants;
            return this;
        }

        public Builder withExcludedVariants(Optional<List<ChannelVariantRef>> excludedVariants) {
            this.excludedVariants = excludedVariants;
            return this;
        }

        public Builder withResolvedEquivalents(Optional<Iterable<Channel>> equivalents) {
            this.equivalents = equivalents;
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
                    excludedVariants,
                    equivalents
            );
        }

        public static Builder copyOf(ResolvedChannel resolvedChannel) {
            return new Builder()
                    .withChannel(resolvedChannel.getChannel())
                    .withParentChannel(resolvedChannel.getParentChannel())
                    .withChannelVariations(resolvedChannel.getChannelVariations())
                    .withChannelGroupSummaries(resolvedChannel.getChannelGroupSummaries())
                    .withChannelGroupMembership(resolvedChannel.getChannelGroupMembership())
                    .withIncludedVariants(resolvedChannel.getIncludedVariants())
                    .withExcludedVariants(resolvedChannel.getExcludedVariants());
        }
    }

}
