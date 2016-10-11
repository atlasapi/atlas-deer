package org.atlasapi.channel;

import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannel {

    private final Channel channel;
    private final Optional<List<ChannelGroupSummary>> channelGroupSummaries;
    private final Optional<Channel> parentChannel;
    private final Optional<Iterable<Channel>> channelVariations;
    private Optional<ChannelGroupMembership> channelGroupMembership;

    private ResolvedChannel(
            Channel channel,
            Optional<List<ChannelGroupSummary>> channelGroupSummaries,
            Optional<Channel> parentChannel,
            Optional<Iterable<Channel>> channelVariations,
            Optional<ChannelGroupMembership> channelGroupMembership
    ) {
        this.channel = checkNotNull(channel);
        this.channelGroupSummaries = checkNotNull(channelGroupSummaries);
        this.parentChannel = checkNotNull(parentChannel);
        this.channelVariations = checkNotNull(channelVariations);
        this.channelGroupMembership = checkNotNull(channelGroupMembership);
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

    public static class Builder {

        private final Channel channel;
        private Optional<List<ChannelGroupSummary>> channelGroupSummaries = Optional.absent();
        private Optional<Channel> parentChannel = Optional.absent();
        private Optional<Iterable<Channel>> channelVariations = Optional.absent();
        private Optional<ChannelGroupMembership> channelGroupMembership = Optional.absent();

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

        public ResolvedChannel build() {
            return new ResolvedChannel(
                    channel,
                    channelGroupSummaries,
                    parentChannel,
                    channelVariations,
                    channelGroupMembership
            );
        }

        public static Builder copyOf(ResolvedChannel resolvedChannel) {
            return new Builder(resolvedChannel.getChannel())
                    .withParentChannel(resolvedChannel.getParentChannel())
                    .withChannelVariations(resolvedChannel.getChannelVariations())
                    .withChannelGroupSummaries(resolvedChannel.getChannelGroupSummaries())
                    .withChannelGroupMembership(resolvedChannel.getChannelGroupMembership());
        }
    }

}
