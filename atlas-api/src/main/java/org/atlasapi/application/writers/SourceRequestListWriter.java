package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.SourceRequest;
import org.atlasapi.application.UsageType;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class SourceRequestListWriter implements EntityListWriter<SourceRequest> {

    private final EntityListWriter<Publisher> sourcesWriter;
    private final EntityWriter<UsageType> usageTypeWriter;
    private final NumberToShortStringCodec idCodec;

    public SourceRequestListWriter(SourceIdCodec sourceIdCodec, NumberToShortStringCodec idCodec) {
        this.sourcesWriter = new SourceWithIdWriter(sourceIdCodec, "source", "sources");
        this.usageTypeWriter = new UsageTypeWriter();
        this.idCodec = idCodec;
    }

    @Override
    public void write(SourceRequest entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("application_id", idCodec.encode(entity.getAppId().toBigInteger()));
        writer.writeField("app_url", entity.getAppUrl());
        writer.writeField("email", entity.getEmail());
        writer.writeObject(sourcesWriter, entity.getSource(), ctxt);
        writer.writeField("reason", entity.getReason());
        writer.writeObject(usageTypeWriter, entity.getUsageType(), ctxt);
        writer.writeField("approved", entity.isApproved());
        writer.writeField("licenseAccepted", entity.isLicenseAccepted());
        writer.writeField("requestedAt", entity.getRequestedAt());
        writer.writeField("approvedAt", entity.getApprovedAt().orNull());
    }

    @Override
    public String fieldName(SourceRequest entity) {
        return "source_request";
    }

    @Override
    public String listName() {
        return "source_requests";
    }

}
