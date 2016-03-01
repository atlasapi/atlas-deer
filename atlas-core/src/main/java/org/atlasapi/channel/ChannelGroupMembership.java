package org.atlasapi.channel;

import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ChannelGroupMembership {

    private final ChannelGroupRef channelGroup;
    private final ChannelRef channel;
    private final Optional<LocalDate> startDate;
    private final Optional<LocalDate> endDate;

    public ChannelGroupMembership(
            ChannelGroupRef channelGroup,
            ChannelRef channelRef,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate
    ) {
        this.channelGroup = checkNotNull(channelGroup);
        this.channel = checkNotNull(channelRef);
        this.startDate = Optional.ofNullable(startDate);
        this.endDate = Optional.ofNullable(endDate);
    }

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }

    public static class Builder {

        private Long channelId;
        private Long channelGroupId;

        private Publisher publisher;
        private LocalDate startDate;
        private LocalDate endDate;
        private String channelNumber;

        public Builder(Publisher publisher) {
            this.publisher = checkNotNull(publisher);
        }

        public Builder withChannelId(Long channelId) {
            this.channelId = channelId;
            return this;
        }

        public Builder withChannelGroupId(Long channelGroupId) {
            this.channelGroupId = channelGroupId;
            return this;
        }

        public Builder withStartDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        public Builder withEndDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

        public Builder withChannelNumber(String channelNumber) {
            this.channelNumber = channelNumber;
            return this;
        }

        public ChannelGroupMembership build() {
            ChannelGroupRef channelGroupRef = new ChannelGroupRef(
                    Id.valueOf(channelGroupId),
                    publisher
            );

            ChannelRef channelRef = new ChannelRef(
                    Id.valueOf(channelId),
                    publisher
            );
            if (isNullOrEmpty(this.channelNumber)) {
                return new ChannelGroupMembership(
                        channelGroupRef,
                        channelRef,
                        startDate,
                        endDate
                );
            }
            return new ChannelNumbering(
                    channelGroupRef,
                    channelRef,
                    startDate,
                    endDate,
                    channelNumber
            );
        }

        public ChannelNumbering buildChannelNumbering() {
            ChannelGroupRef channelGroupRef = new ChannelGroupRef(
                    Id.valueOf(channelGroupId),
                    publisher
            );

            ChannelRef channelRef = new ChannelRef(
                    Id.valueOf(channelId),
                    publisher
            );
            return new ChannelNumbering(
                    channelGroupRef,
                    channelRef,
                    startDate,
                    endDate,
                    channelNumber
            );
        }

    }

    @FieldName("channel_group")
    public ChannelGroupRef getChannelGroup() {
        return channelGroup;
    }

    @FieldName("channel")
    public ChannelRef getChannel() {
        return channel;
    }

    @FieldName("start_date")
    public Optional<LocalDate> getStartDate() {
        return startDate;
    }

    @FieldName("end_date")
    public Optional<LocalDate> getEndDate() {
        return endDate;
    }

    // We only have dates and no times for channel availability start/end
    // so we will assume a channel is available during the entire start date,
    // but not during the end date to avoid showing multiple versions of the
    // same channel showing up at the same time. This is to work around PA
    // not providing exact channel availability times
    public boolean isAvailable(LocalDate date) {
        LocalDate startDate = this.startDate.orElse(date.minusDays(1));
        LocalDate endDate = this.endDate.orElse(date.plusDays(1));

        return (startDate.isBefore(date) || startDate.isEqual(date)) && endDate.isAfter(date);
    }
}
