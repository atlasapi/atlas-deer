package org.atlasapi.channel;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelGroup {

    private final ChannelGroup<?> channelGroup;
    private final Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
    private final Optional<ChannelGroup<?>> platformChannelGroup;
    private final Optional<ChannelGroup<?>> channelNumbersFromGroup;
    private final Optional<Iterable<ResolvedChannel>> channels;

    private ResolvedChannelGroup(
            ChannelGroup channelGroup,
            Optional<Iterable<ChannelGroup<?>>> regionChannelGroups,
            Optional<ChannelGroup<?>> platformChannelGroup,
            Optional<ChannelGroup<?>> channelNumbersFromGroup,
            Optional<Iterable<ResolvedChannel>> channels
    ) {
        this.channelGroup = checkNotNull(channelGroup);
        this.regionChannelGroups = checkNotNull(regionChannelGroups);
        this.platformChannelGroup = checkNotNull(platformChannelGroup);
        this.channelNumbersFromGroup = checkNotNull(channelNumbersFromGroup);
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

    public Optional<ChannelGroup<?>> getChannelNumbersFromGroup() {
        return channelNumbersFromGroup;
    }

    public Optional<Iterable<ResolvedChannel>> getChannels() {
        return channels;
    }

    public static class Builder {

        private final ChannelGroup<?> channelGroup;
        private Optional<Iterable<ChannelGroup<?>>> regionChannelGroups = Optional.empty();
        private Optional<ChannelGroup<?>> platformChannelGroup = Optional.empty();
        private Optional<ChannelGroup<?>> channelNumbersFromGroup = Optional.empty();
        private Optional<Iterable<ResolvedChannel>> channels = Optional.empty();

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

        public Builder withChannelNumbersFromGroup(Optional<ChannelGroup<?>> channelNumbersFromGroup) {
            this.channelNumbersFromGroup = channelNumbersFromGroup;
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
                    channelNumbersFromGroup,
                    channels
            );
        }
    }

}
