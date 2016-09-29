package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.IdSummaryWriter;

import com.google.common.base.Throwables;

public class ChannelGroupIdSummaryAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    IdSummaryWriter idSummaryWriter;
    public ChannelGroupIdSummaryAnnotation(IdSummaryWriter idSummaryWriter) {
        this.idSummaryWriter = idSummaryWriter;
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt) {
        try {
            idSummaryWriter.write(entity.getChannelGroup(), writer, ctxt);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

}
