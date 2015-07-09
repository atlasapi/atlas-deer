package org.atlasapi.channel;

import org.joda.time.LocalDate;

import javax.annotation.Nullable;
import javax.swing.text.html.Option;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

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
