package org.atlasapi.content;

import java.util.Date;

import org.atlasapi.util.EsObject;

public class EsBroadcast extends EsObject {
    
    public final static String ID = "id";
    public final static String CHANNEL = "channel";
    public final static String TRANSMISSION_TIME = "transmissionTime";
    public final static String TRANSMISSION_END_TIME = "transmissionEndTime";
    public final static String TRANSMISSION_TIME_IN_MILLIS = "transmissionTimeInMillis";
    public final static String REPEAT = "repeat";
    
    public EsBroadcast id(String id) {
        properties.put(ID, id);
        return this;
    }
    
    public EsBroadcast channel(Long channel) {
        properties.put(CHANNEL, channel);
        return this;
    }
    
    public EsBroadcast transmissionTime(Date transmissionTime) {
        properties.put(TRANSMISSION_TIME, transmissionTime);
        return this;
    }
    
    public EsBroadcast transmissionEndTime(Date transmissionEndTime) {
        properties.put(TRANSMISSION_END_TIME, transmissionEndTime);
        return this;
    }
    
    public EsBroadcast repeat(Boolean repeat) {
        properties.put(REPEAT, repeat);
        return this;
    }
}
