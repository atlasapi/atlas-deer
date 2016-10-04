package org.atlasapi.channel;

import org.atlasapi.output.ChannelWithChannelGroupMembership;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelGroup {

    private final ChannelGroup<?> channelGroup;
    private final Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
    private final Optional<ChannelGroup<?>> platformChannelGroup;
    private final Optional<ImmutableSet<ChannelWithChannelGroupMembership>> advertisedChannels;

    private ResolvedChannelGroup(
            ChannelGroup channelGroup,
            Optional<Iterable<ChannelGroup<?>>> regionChannelGroups,
            Optional<ChannelGroup<?>> platformChannelGroup,
            Optional<ImmutableSet<ChannelWithChannelGroupMembership>> advertisedChannels
            ) {
        this.channelGroup = channelGroup;
        this.regionChannelGroups = regionChannelGroups;
        this.platformChannelGroup = platformChannelGroup;
        this.advertisedChannels = advertisedChannels;
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

    public Optional<ImmutableSet<ChannelWithChannelGroupMembership>> getAdvertisedChannels() {
        return advertisedChannels;
    }

    public static class Builder {

        ChannelGroup<?> channelGroup;
        Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;
        Optional<ChannelGroup<?>> platformChannelGroup;
        Optional<ImmutableSet<ChannelWithChannelGroupMembership>> advertisedChannels;

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

        public Builder withAdvertisedChannels(Optional<ImmutableSet<ChannelWithChannelGroupMembership>> advertisedChannels) {
            this.advertisedChannels = advertisedChannels;
            return this;
        }

        public ResolvedChannelGroup build() {
            return new ResolvedChannelGroup(
                    channelGroup,
                    regionChannelGroups,
                    platformChannelGroup,
                    advertisedChannels
            );
        }
    }

}
