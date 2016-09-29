package org.atlasapi.channel;

import javax.annotation.Nullable;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResolvedChannelGroup {

    private final ChannelGroup channelGroup;
    private Optional<Iterable<ChannelGroup<?>>> regionChannelGroups;

    private ResolvedChannelGroup(ChannelGroup channelGroup) {
        this.channelGroup = checkNotNull(channelGroup);
    }

    public static ResolvedChannelGroup create(ChannelGroup channelGroup) {
        return new ResolvedChannelGroup(channelGroup);
    }

    public void setRegions(@Nullable Iterable<ChannelGroup<?>> regionChannelGroups) {
        this.regionChannelGroups = Optional.fromNullable(regionChannelGroups);
    }

    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }

    public Optional<Iterable<ChannelGroup<?>>> getRegionChannelGroups() {
        return regionChannelGroups;
    }

}
