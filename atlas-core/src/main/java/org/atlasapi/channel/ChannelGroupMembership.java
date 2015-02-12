package org.atlasapi.channel;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.LocalDate;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ChannelGroupMembership {

    private final ChannelGroupRef channelGroup;
    private final ChannelRef channel;
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
        this.channelGroup = checkNotNull(channelGroup);
        this.channel = checkNotNull(channelRef);
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
            if(isNullOrEmpty(this.channelNumber)) {
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
    public LocalDate getStartDate() {
        return startDate;
    }

    @FieldName("end_date")
    public LocalDate getEndDate() {
        return endDate;
    }
}
