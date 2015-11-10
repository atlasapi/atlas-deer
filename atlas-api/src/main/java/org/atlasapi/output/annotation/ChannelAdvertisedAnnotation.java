package org.atlasapi.output.annotation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channel.ChannelWriter;
import org.joda.time.DateTime;

public class ChannelAdvertisedAnnotation extends OutputAnnotation<Channel> {
    private final ChannelWriter channelWriter;

    public ChannelAdvertisedAnnotation(ChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        DateTime advertiseFrom = entity.getAdvertiseFrom();
        if (advertiseFrom != null && isInFuture(advertiseFrom)) {
            channelWriter.write(entity, format, ctxt);
        }
    }

    private boolean isInFuture(DateTime dateTime) {
        return dateTime.isAfter(DateTime.now());
    }

}
