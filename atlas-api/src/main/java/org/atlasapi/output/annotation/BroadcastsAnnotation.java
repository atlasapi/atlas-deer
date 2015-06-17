package org.atlasapi.output.annotation;

import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.stream.Stream;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

public class BroadcastsAnnotation extends OutputAnnotation<Content> {
    
    private final BroadcastWriter broadcastWriter;
    
    public BroadcastsAnnotation(NumberToShortStringCodec codec) {
        broadcastWriter = new BroadcastWriter("broadcasts", codec);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(FieldWriter writer, Item item, OutputContext ctxt) throws IOException {
        Stream<Broadcast> broadcastStream = item.getBroadcasts().stream()
                .filter(b -> ACTIVELY_PUBLISHED.apply(b));
        Map params = ctxt.getRequest().getParameterMap();

        if (params.containsKey("broadcasts.transmissionStartTime.gt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionStartTime.gt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionTime().isAfter(time));
            }
        }

        if (params.containsKey("broadcasts.transmissionStartTime.lt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionStartTime.lt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionTime().isBefore(time));
            }
        }

        if (params.containsKey("broadcasts.transmissionEndTime.gt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionEndTime.gt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionEndTime().isAfter(time));
            }
        }

        if (params.containsKey("broadcasts.transmissionStartTime.gt")) {
            String[] paramVals = (String[]) params.get("broadcasts.transmissionStartTime.lt");
            if (paramVals.length > 1 && paramVals[0] != null) {
                DateTime time = DateTime.parse(paramVals[0]);
                broadcastStream.filter(b -> b.getTransmissionEndTime().isBefore(time));
            }
        }

        writer.writeList(broadcastWriter, broadcastStream.collect(ImmutableCollectors.toList()), ctxt);
    }

}
