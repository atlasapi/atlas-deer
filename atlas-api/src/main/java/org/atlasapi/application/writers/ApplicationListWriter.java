package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SourceWriter;
import org.atlasapi.query.common.Resource;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class ApplicationListWriter implements EntityListWriter<Application> {

    private final ApplicationCredentialsWriter credentialsWriter;
    private final EntityListWriter<ApplicationSources> sourcesWriter;
    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("publisher");
    private final NumberToShortStringCodec idCodec;
    
    public ApplicationListWriter(NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec) {
        this.idCodec = idCodec;
        this.credentialsWriter =  new ApplicationCredentialsWriter();
        this.sourcesWriter = new ApplicationSourcesWriter(sourceIdCodec);
    }

    @Override
    public void write(Application entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        ctxt.startResource(Resource.APPLICATION);
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("revoked", entity.isRevoked());
        writer.writeField("title", entity.getTitle());
        writer.writeField("description", entity.getDescription());
        writer.writeField("created", entity.getCreated());
        writer.writeObject(credentialsWriter, entity.getCredentials(), ctxt);
        writer.writeObject(sourcesWriter, entity.getSources(), ctxt);
        writer.writeObject(publisherWriter, entity.getSource(), ctxt);
        ctxt.endResource();

    }

    @Override
    public String fieldName(Application entity) {
        return entity.getClass().getSimpleName().toLowerCase();
    }

    @Override
    public String listName() {
        return "applications";
    }

}
