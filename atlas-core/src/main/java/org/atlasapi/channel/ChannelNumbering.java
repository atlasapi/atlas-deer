package org.atlasapi.channel;

import java.util.Optional;

import javax.annotation.Nullable;

import org.joda.time.LocalDate;

public class ChannelNumbering extends ChannelGroupMembership {

    private final Optional<String> channelNumber;

    public ChannelNumbering(
            ChannelGroupRef channelGroup,
            ChannelRef channelRef,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate,
            @Nullable String channelNumber
    ) {
        super(channelGroup, channelRef, startDate, endDate);
        this.channelNumber = Optional.ofNullable(channelNumber);
    }

    public Optional<String> getChannelNumber() {
        return channelNumber;
    }
}
