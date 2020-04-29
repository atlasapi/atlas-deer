package org.atlasapi.query.v5.search.attribute;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class SherlockAttributes {

    public static final String YEAR_PARAM = "filter.year";
    public static final String TYPE_PARAM = "filter.type";
    public static final String PUBLISHER_PARAM = "filter.publisher";
    public static final String SCHEDULE_UPCOMING_PARAM = "filter.schedule.upcoming";
    public static final String SCHEDULE_TIME_PARAM = "filter.schedule.time";
    public static final String SCHEDULE_CHANNEL_PARAM = "filterOption.schedule.channel";
    public static final String SCHEDULE_CHANNEL_GROUP_PARAM = "filterOption.schedule.channelGroup";
    public static final String ON_DEMAND_AVAILABLE_PARAM = "filter.ondemand.available";

    public static List<String> all() {
        return ImmutableList.<String>builder()
                .add(YEAR_PARAM)
                .add(TYPE_PARAM)
                .add(PUBLISHER_PARAM)
                .add(SCHEDULE_UPCOMING_PARAM)
                .add(SCHEDULE_TIME_PARAM)
                .add(SCHEDULE_CHANNEL_PARAM)
                .add(SCHEDULE_CHANNEL_GROUP_PARAM)
                .add(ON_DEMAND_AVAILABLE_PARAM)
                .build();
    }
}
