package org.atlasapi.query.v4.organisation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.organisation.Organisation;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

public class OrganisationQueryResultWriter extends QueryResultWriter<Organisation> {

    private final EntityListWriter<Organisation> organisationListWriter;

    public OrganisationQueryResultWriter(
            EntityListWriter<Organisation> eventListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.organisationListWriter = checkNotNull(eventListWriter);
    }

    @Override
    protected void writeResult(QueryResult<Organisation> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Organisation> events = result.getResources();
            writer.writeList(organisationListWriter, events, ctxt);
        } else {
            writer.writeObject(organisationListWriter, result.getOnlyResource(), ctxt);
        }
    }
}
