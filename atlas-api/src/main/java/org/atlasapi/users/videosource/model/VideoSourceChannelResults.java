package org.atlasapi.users.videosource.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;

public class VideoSourceChannelResults {

    private final String id;
    private final Iterable<VideoSourceChannel> channels;

    private VideoSourceChannelResults(String id, Iterable<VideoSourceChannel> channels) {
        this.id = id;
        this.channels = ImmutableList.copyOf(channels);
    }

    public String getId() {
        return id;
    }

    public Iterable<VideoSourceChannel> getChannels() {
        return channels;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private Iterable<VideoSourceChannel> channels = Lists.newLinkedList();

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withChannels(Iterable<VideoSourceChannel> channels) {
            this.channels = channels;
            return this;
        }

        public VideoSourceChannelResults build() {
            Preconditions.checkNotNull(id);
            Preconditions.checkNotNull(channels);
            return new VideoSourceChannelResults(id, channels);
        }
    }

}
