package org.atlasapi.content;

import java.util.Date;
import java.util.Map;

import org.atlasapi.entity.Alias;
import org.atlasapi.util.EsAlias;
import org.atlasapi.util.EsObject;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

/**
 */
public class EsLocation extends EsObject {

    public final static String AVAILABILITY_TIME = "availabilityTime";
    public final static String AVAILABILITY_END_TIME = "availabilityEndTime";
    public final static String ALIASES = "aliases";

    public EsLocation availabilityTime(Date availabilityTime) {
        properties.put(AVAILABILITY_TIME, availabilityTime);
        return this;
    }

    public EsLocation availabilityEndTime(Date availabilityEndTime) {
        properties.put(AVAILABILITY_END_TIME, availabilityEndTime);
        return this;
    }

    public EsLocation aliases(Iterable<Alias> aliases) {
        properties.put(
                ALIASES,
                Iterables.transform(aliases, Functions.compose(TO_MAP, EsAlias.toEsAlias()))
        );
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

        if (map.get(ALIASES) != null) {
            esLocation.aliases((Iterable<Alias>)map.get(ALIASES));
        }
        return esLocation;
    }
}