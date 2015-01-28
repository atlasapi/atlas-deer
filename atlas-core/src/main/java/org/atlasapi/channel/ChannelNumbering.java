package org.atlasapi.channel;

import com.google.common.base.Preconditions;
import org.joda.time.LocalDate;

import javax.annotation.Nullable;

public class ChannelNumbering extends ChannelGroupMembership {

    private final String channelNumber;

    public ChannelNumbering(
            ChannelGroupRef channelGroup,
            ChannelRef channelRef,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate,
            String channelNumber
    ) {
        super(channelGroup, channelRef, startDate, endDate);
        Preconditions.checkNotNull(channelNumber);
        this.channelNumber = channelNumber;
    }
}
