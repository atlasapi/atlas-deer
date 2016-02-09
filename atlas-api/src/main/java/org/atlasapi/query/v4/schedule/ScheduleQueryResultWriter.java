package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;

import com.google.common.collect.FluentIterable;

public class ScheduleQueryResultWriter extends QueryResultWriter<ChannelSchedule> {

    private final EntityListWriter<ChannelSchedule> scheduleWriter;

    public ScheduleQueryResultWriter(
            EntityListWriter<ChannelSchedule> scheduleWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.scheduleWriter = scheduleWriter;
    }

    @Override
    protected void writeResult(QueryResult<ChannelSchedule> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<ChannelSchedule> resources = result.getResources();
            writer.writeList(scheduleWriter, resources, ctxt);
        } else {
            writer.writeObject(scheduleWriter, result.getOnlyResource(), ctxt);
        }
    }

}
