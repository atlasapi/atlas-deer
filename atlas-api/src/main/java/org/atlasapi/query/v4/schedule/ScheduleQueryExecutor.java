package org.atlasapi.query.v4.schedule;

import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.schedule.ChannelSchedule;

public interface ScheduleQueryExecutor {

    QueryResult<ChannelSchedule> execute(ScheduleQuery scheduleQuery)
            throws QueryExecutionException;

}
