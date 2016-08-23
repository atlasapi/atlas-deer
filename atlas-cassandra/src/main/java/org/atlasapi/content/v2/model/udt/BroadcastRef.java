package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "broadcastref")
public class BroadcastRef {

    @Field(name = "src_id") private String sourceId;
    @Field(name = "channel_id") private Long channelId;
    @Field(name = "start") private Instant start;
    @Field(name = "end") private Instant end;

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

    public Instant getStart() {
        return start;
    }

    public void setStart(Instant start) {
        this.start = start;
    }

    public Instant getEnd() {
        return end;
    }

    public void setEnd(Instant end) {
        this.end = end;
    }
}
