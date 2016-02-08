package org.atlasapi.application.writers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.EndUserLicense;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

public class EndUserLicenseQueryResultWriter extends QueryResultWriter<EndUserLicense> {

    private final EntityListWriter<EndUserLicense> endUserLicenseListWriter;

    public EndUserLicenseQueryResultWriter(
            EntityListWriter<EndUserLicense> endUserLicenseListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.endUserLicenseListWriter = endUserLicenseListWriter;
    }

    @Override
    protected void writeResult(QueryResult<EndUserLicense> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<EndUserLicense> resources = result.getResources();
            writer.writeList(endUserLicenseListWriter, resources, ctxt);
        } else {
            writer.writeObject(endUserLicenseListWriter, result.getOnlyResource(), ctxt);
        }
    }

}
