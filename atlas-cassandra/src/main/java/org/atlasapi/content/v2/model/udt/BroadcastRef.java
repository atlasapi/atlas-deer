package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "broadcastref")
public class BroadcastRef {

    @Field(name = "src_id") private String sourceId;
    @Field(name = "channel_id") private Long channelId;
    @Field(name = "interval") @Frozen private Interval interval;

    public BroadcastRef() {}

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public Long getChannelId() {
        return channelId;
    }

    public void setChannelId(Long channelId) {
        this.channelId = channelId;
    }

    public Interval getInterval() {
        return interval;
    }

    public void setInterval(Interval interval) {
        this.interval = interval;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        BroadcastRef that = (BroadcastRef) object;
        return Objects.equals(sourceId, that.sourceId) &&
                Objects.equals(channelId, that.channelId) &&
                Objects.equals(interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, channelId, interval);
    }
}
