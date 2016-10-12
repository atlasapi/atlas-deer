package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.IdSummaryWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIdSummaryAnnotation extends OutputAnnotation<ResolvedChannel> {

    private final IdSummaryWriter idSummaryWriter;

    private ChannelIdSummaryAnnotation(IdSummaryWriter idSummaryWriter) {
        this.idSummaryWriter = checkNotNull(idSummaryWriter);
    }

    public static ChannelIdSummaryAnnotation create(IdSummaryWriter idSummaryWriter) {
        return new ChannelIdSummaryAnnotation(idSummaryWriter);
    }

    @Override
    public void write(ResolvedChannel entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        idSummaryWriter.write(entity.getChannel(), writer, ctxt);
    }

}
