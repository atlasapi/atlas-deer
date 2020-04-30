package org.atlasapi.query.v5.search.attribute;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum SherlockParameter {

    YEAR(ParameterType.FILTER, "year"),
    TYPE(ParameterType.FILTER, "type"),
    PUBLISHER(ParameterType.FILTER, "publisher"),
    SCHEDULE_UPCOMING(ParameterType.FILTER, "schedule.upcoming"),
    SCHEDULE_TIME(ParameterType.FILTER, "schedule.time"),
    SCHEDULE_CHANNEL(ParameterType.FILTER, "schedule.channel"),
    SCHEDULE_CHANNEL_GROUP(ParameterType.FILTER, "schedule.channelGroup"),
    ON_DEMAND_AVAILABLE(ParameterType.FILTER, "ondemand.available"),
    ;

    private String name;
    SherlockParameter(ParameterType type, String name) {
        this.name = type.name().toLowerCase() + "." + name;
    }

    public String getName() {
        return name;
    }

    public static Set<String> getAllNames() {
        return Arrays.stream(SherlockParameter.values())
                .map(SherlockParameter::getName)
                .collect(Collectors.toSet());
    }

    enum ParameterType {
        FILTER
    }
}
