package org.atlasapi.channel;

import org.joda.time.LocalDate;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelNumbering extends ChannelGroupMembership {

    private final String channelNumber;

    public ChannelNumbering(
            ChannelGroupRef channelGroup,
            ChannelRef channelRef,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate,
            @Nullable String channelNumber
    ) {
        super(channelGroup, channelRef, startDate, endDate);
        this.channelNumber = channelNumber;
    }

    public String getChannelNumber() {
        return channelNumber;
    }
}
