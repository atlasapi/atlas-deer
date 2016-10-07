package org.atlasapi.channel;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelGroup {

    private final ChannelGroup<?> channelGroup;
    private final Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
    private final Optional<ChannelGroup<?>> platformChannelGroup;
    private final Optional<Iterable<ResolvedChannel>> channels;

    private ResolvedChannelGroup(
            ChannelGroup channelGroup,
            Optional<Iterable<ChannelGroup<?>>> regionChannelGroups,
            Optional<ChannelGroup<?>> platformChannelGroup,
            Optional<Iterable<ResolvedChannel>> channels
            ) {
        this.channelGroup = checkNotNull(channelGroup);
        this.regionChannelGroups = checkNotNull(regionChannelGroups);
        this.platformChannelGroup = checkNotNull(platformChannelGroup);
        this.channels = checkNotNull(channels);
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

    public Optional<Iterable<ResolvedChannel>> getChannels() {
        return channels;
    }

    public static class Builder {

        private final ChannelGroup<?> channelGroup;
        private Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
        private Optional<ChannelGroup<?>> platformChannelGroup;
        private Optional<Iterable<ResolvedChannel>> channels;

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

        public Builder withAdvertisedChannels(Optional<Iterable<ResolvedChannel>> channels) {
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
