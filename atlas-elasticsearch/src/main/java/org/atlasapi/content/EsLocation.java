package org.atlasapi.content;

import java.util.Date;
import java.util.Map;

import org.atlasapi.util.EsObject;
import org.joda.time.DateTime;

/**
 */
public class EsLocation extends EsObject {
    
    public final static String AVAILABILITY_TIME = "availabilityTime";
    public final static String AVAILABILITY_END_TIME = "availabilityEndTime";
    
    
    public EsLocation availabilityTime(Date availabilityTime) {
        properties.put(AVAILABILITY_TIME, availabilityTime);
        return this;
    }
    
    public EsLocation availabilityEndTime(Date availabilityEndTime) {
        properties.put(AVAILABILITY_END_TIME, availabilityEndTime);
        return this;
    }

    public static EsLocation fromMap(Map<String, Object> map) {
        EsLocation esLocation = new EsLocation();
        if (map.get(AVAILABILITY_TIME) != null) {
            esLocation.availabilityTime(new DateTime(map.get(AVAILABILITY_TIME)).toDate());
        }
        if (map.get(AVAILABILITY_END_TIME) != null) {
            esLocation.availabilityEndTime(new DateTime(map.get(AVAILABILITY_END_TIME)).toDate());
        }
        return esLocation;
    }
}
