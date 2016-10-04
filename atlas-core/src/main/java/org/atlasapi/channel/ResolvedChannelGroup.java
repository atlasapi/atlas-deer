package org.atlasapi.channel;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelGroup {

    private final ChannelGroup channelGroup;
    private final Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;

    private ResolvedChannelGroup(
            ChannelGroup channelGroup,
            Optional<Iterable<ChannelGroup<?>>> regionChannelGroups
            ) {
        this.channelGroup = channelGroup;
        this.regionChannelGroups = regionChannelGroups;
    }

    public static Builder builder(ChannelGroup channelGroup) {
        return new Builder(channelGroup);
    }

    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }

    public Optional<Iterable<ChannelGroup<?>>> getRegionChannelGroups() {
        return regionChannelGroups;
    }

    public static class Builder {

        ChannelGroup channelGroup;
        Optional<Iterable<ChannelGroup<?>>> regionChannelGroup;

        public Builder(ChannelGroup channelGroup) {
            this.channelGroup = checkNotNull(channelGroup);
        }

        public Builder withRegionChannelGroup(Optional<Iterable<ChannelGroup<?>>> regionChannelGroup) {
            this.regionChannelGroup = regionChannelGroup;
            return this;
        }

        public ResolvedChannelGroup build() {
            return new ResolvedChannelGroup(
                    channelGroup,
                    regionChannelGroup
            );
        }
    }

}
