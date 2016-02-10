package org.atlasapi.users.videosource;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.users.videosource.model.VideoSourceChannel;

public class VideoSourceChannelWriter implements EntityListWriter<VideoSourceChannel> {

    @Override
    public void write(VideoSourceChannel entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("id", entity.getId());
        writer.writeField("title", entity.getTitle());
        writer.writeField("image_url", entity.getImageUrl());
    }

    @Override
    public String fieldName(VideoSourceChannel entity) {
        return "channels";
    }

    @Override
    public String listName() {
        return "channels";
    }

}
