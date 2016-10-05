package org.atlasapi.channel;

import java.util.List;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannel {

    private final Channel channel;
    private final Optional<List<ChannelGroupSummary>> channelGroupSummaries;
    private final Optional<Channel> parentChannel;
    private final Optional<Iterable<Channel>> channelVariations;

    private ResolvedChannel(
            Channel channel,
            Optional<List<ChannelGroupSummary>> channelGroupSummaries,
            Optional<Channel> parentChannel,
            Optional<Iterable<Channel>> channelVariations
    ) {
        this.channel = checkNotNull(channel);
        this.channelGroupSummaries = channelGroupSummaries;
        this.parentChannel = parentChannel;
        this.channelVariations = channelVariations;
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

    public static class Builder {

        private Channel channel;
        private Optional<List<ChannelGroupSummary>> channelGroupSummaries;
        private Optional<Channel> parentChannel;
        private Optional<Iterable<Channel>> channelVariations;

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

        public ResolvedChannel build() {
            return new ResolvedChannel(
                    channel,
                    channelGroupSummaries,
                    parentChannel,
                    channelVariations
            );
        }
    }

}
