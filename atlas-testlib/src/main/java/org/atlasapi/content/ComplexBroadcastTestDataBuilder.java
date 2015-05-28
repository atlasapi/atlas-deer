package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class ComplexBroadcastTestDataBuilder {
    private Id channel;
    private DateTime startTime;
    private Duration duration;
    
    public static ComplexBroadcastTestDataBuilder broadcast() {
        return new ComplexBroadcastTestDataBuilder();
    }
    
    private ComplexBroadcastTestDataBuilder() {
        channel = Id.valueOf(1);
        startTime = new DateTime();
        duration = Duration.standardMinutes(60);
    }
    
    public Broadcast build() {
        Broadcast broadcast = new Broadcast(channel, startTime, duration);
        
        return broadcast;
    }
    
    public ComplexBroadcastTestDataBuilder withChannel(Id channel) {
        this.channel = channel;
        return this;
    }
    
    public ComplexBroadcastTestDataBuilder withStartTime(DateTime startTime) {
        this.startTime = startTime;
        return this;
    }
    
    public ComplexBroadcastTestDataBuilder withDuration(Duration duration) {
        this.duration = duration;
        return this;
    }
}
