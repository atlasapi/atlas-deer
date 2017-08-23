package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.IdSummaryWriter;

import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupIdSummaryAnnotation extends OutputAnnotation<ResolvedChannelGroup, ResolvedChannelGroup> {

    private final IdSummaryWriter idSummaryWriter;

    private ChannelGroupIdSummaryAnnotation(IdSummaryWriter idSummaryWriter) {
        this.idSummaryWriter = checkNotNull(idSummaryWriter);
    }

    public static ChannelGroupIdSummaryAnnotation create(IdSummaryWriter idSummaryWriter) {
        return new ChannelGroupIdSummaryAnnotation(idSummaryWriter);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        idSummaryWriter.write(entity.getChannelGroup(), writer, ctxt);
    }

}
