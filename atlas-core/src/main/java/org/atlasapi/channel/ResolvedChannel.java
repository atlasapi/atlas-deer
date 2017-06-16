package org.atlasapi.channel;

import java.util.List;
import java.util.Optional;

import org.atlasapi.content.ChannelVariantRef;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannel {

    private final Channel channel;
    private final List<ChannelGroupSummary> channelGroupSummaries;
    private final Channel parentChannel;
    private final Iterable<Channel> channelVariations;
    private final List<ChannelVariantRef> includedVariants;
    private final List<ChannelVariantRef> excludedVariants;
    private final ChannelGroupMembership channelGroupMembership;
    private final Iterable<Channel> equivalents;

    private ResolvedChannel(
            Channel channel,
            List<ChannelGroupSummary> channelGroupSummaries,
            Channel parentChannel,
            Iterable<Channel> channelVariations,
            ChannelGroupMembership channelGroupMembership,
            List<ChannelVariantRef> includedVariants,
            List<ChannelVariantRef> excludedVariants,
            Iterable<Channel> equivalents
    ) {
        this.channel = checkNotNull(channel);
        this.channelGroupSummaries = channelGroupSummaries;
        this.parentChannel = parentChannel;
        this.channelVariations = channelVariations;
        this.channelGroupMembership = channelGroupMembership;
        this.includedVariants = includedVariants;
        this.excludedVariants = excludedVariants;
        this.equivalents = equivalents;
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
        return Optional.ofNullable(channelGroupSummaries);
    }

    public Optional<Channel> getParentChannel() {
        return Optional.ofNullable(parentChannel);
    }

    public Optional<Iterable<Channel>> getChannelVariations() {
        return Optional.ofNullable(channelVariations);
    }

    public Optional<ChannelGroupMembership> getChannelGroupMembership() {
        return Optional.ofNullable(channelGroupMembership);
    }

    public Optional<List<ChannelVariantRef>> getIncludedVariants() {
        return Optional.ofNullable(includedVariants);
    }

    public Optional<List<ChannelVariantRef>> getExcludedVariants() {
        return Optional.ofNullable(excludedVariants);
    }

    public Optional<Iterable<Channel>> getEquivalents() {
        return Optional.ofNullable(equivalents);
    }

    public static class Builder {

        private Channel channel;
        private List<ChannelGroupSummary> channelGroupSummaries;
        private Channel parentChannel;
        private Iterable<Channel> channelVariations;
        private ChannelGroupMembership channelGroupMembership;
        private List<ChannelVariantRef> includedVariants;
        private List<ChannelVariantRef> excludedVariants;
        private Iterable<Channel> equivalents;

        private Builder(Channel channel) {
            this.channel = channel;
        }

        private Builder () {}

        public Builder withChannel(Channel channel) {
            this.channel = channel;
            return this;
        }

        public Builder withChannelGroupSummaries(List<ChannelGroupSummary> channelGroupSummaries) {
            this.channelGroupSummaries = channelGroupSummaries;
            return this;
        }

        public Builder withParentChannel(Channel parentChannel) {
            this.parentChannel = parentChannel;
            return this;
        }

        public Builder withChannelVariations(Iterable<Channel> channelVariations) {
            this.channelVariations = channelVariations;
            return this;
        }

        public Builder withChannelGroupMembership(ChannelGroupMembership channelGroupMembership) {
            this.channelGroupMembership = channelGroupMembership;
            return this;
        }

        public Builder withIncludedVariants(List<ChannelVariantRef> includedVariants) {
            this.includedVariants = includedVariants;
            return this;
        }

        public Builder withExcludedVariants(List<ChannelVariantRef> excludedVariants) {
            this.excludedVariants = excludedVariants;
            return this;
        }

        public Builder withResolvedEquivalents(@Nullable Iterable<Channel> equivalents) {
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
                    .withParentChannel(resolvedChannel.getParentChannel().orElse(null))
                    .withChannelVariations(resolvedChannel.getChannelVariations().orElse(null))
                    .withChannelGroupSummaries(resolvedChannel.getChannelGroupSummaries().orElse(null))
                    .withChannelGroupMembership(resolvedChannel.getChannelGroupMembership().orElse(null))
                    .withIncludedVariants(resolvedChannel.getIncludedVariants().orElse(null))
                    .withExcludedVariants(resolvedChannel.getExcludedVariants().orElse(null));
        }
    }

}
