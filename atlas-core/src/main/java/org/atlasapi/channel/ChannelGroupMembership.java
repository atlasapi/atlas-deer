package org.atlasapi.channel;

import com.google.common.base.Preconditions;
import org.joda.time.LocalDate;

import javax.annotation.Nullable;

public class ChannelGroupMembership {

    private final ChannelGroupRef channelGroup;
    private final ChannelRef channelRef;
    private final LocalDate startDate;
    private final LocalDate endDate;

    public ChannelGroupMembership(
            ChannelGroupRef channelGroup,
            ChannelRef channelRef,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate
    ) {
        this.startDate = startDate;
        this.endDate = endDate;
        Preconditions.checkNotNull(channelGroup);
        Preconditions.checkNotNull(channelRef);
        this.channelGroup = channelGroup;
        this.channelRef = channelRef;
    }
}
