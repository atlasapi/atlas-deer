package org.atlasapi.users.videosource;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.users.videosource.model.VideoSourceChannel;
import org.atlasapi.users.videosource.model.VideoSourceChannelResults;

public class VideoSourceChannelResultsListWriter implements
        EntityListWriter<VideoSourceChannelResults> {

    private EntityListWriter<VideoSourceChannel> channelWriter = new VideoSourceChannelWriter();

    @Override
    public void write(VideoSourceChannelResults entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("id", entity.getId());
        writer.writeList(channelWriter, entity.getChannels(), ctxt);
    }

    @Override
    public String fieldName(VideoSourceChannelResults entity) {
        return "user";
    }

    @Override
    public String listName() {
        return "user";
    }

}
