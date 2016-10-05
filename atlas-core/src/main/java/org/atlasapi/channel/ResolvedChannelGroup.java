package org.atlasapi.channel;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelGroup {

    private final ChannelGroup<?> channelGroup;
    private final Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
    private final Optional<ChannelGroup<?>> platformChannelGroup;
    private final Optional<Iterable<Channel>> channels;

    private ResolvedChannelGroup(
            ChannelGroup channelGroup,
            Optional<Iterable<ChannelGroup<?>>> regionChannelGroups,
            Optional<ChannelGroup<?>> platformChannelGroup,
            Optional<Iterable<Channel>> channels
            ) {
        this.channelGroup = channelGroup;
        this.regionChannelGroups = regionChannelGroups;
        this.platformChannelGroup = platformChannelGroup;
        this.channels = channels;
    }

    public static Builder builder(ChannelGroup channelGroup) {
        return new Builder(channelGroup);
    }

    public ChannelGroup<?> getChannelGroup() {
        return channelGroup;
    }

    public Optional<Iterable<ChannelGroup<?>>> getRegionChannelGroups() {
        return regionChannelGroups;
    }

    public Optional<ChannelGroup<?>> getPlatformChannelGroup() {
        return platformChannelGroup;
    }

    public Optional<Iterable<Channel>> getChannels() {
        return channels;
    }

    public static class Builder {

        ChannelGroup<?> channelGroup;
        Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
        Optional<ChannelGroup<?>> platformChannelGroup;
        Optional<Iterable<Channel>> channels;

        public Builder(ChannelGroup<?> channelGroup) {
            this.channelGroup = checkNotNull(channelGroup);
        }

        public Builder withRegionChannelGroups(Optional<Iterable<ChannelGroup<?>>> regionChannelGroups) {
            this.regionChannelGroups = regionChannelGroups;
            return this;
        }

        public Builder withPlatformChannelGroup(Optional<ChannelGroup<?>> platformChannelGroup) {
            this.platformChannelGroup = platformChannelGroup;
            return this;
        }

        public Builder withAdvertisedChannels(Optional<Iterable<Channel>> channels) {
            this.channels = channels;
            return this;
        }

        public ResolvedChannelGroup build() {
            return new ResolvedChannelGroup(
                    channelGroup,
                    regionChannelGroups,
                    platformChannelGroup,
                    channels
            );
        }
    }

}
