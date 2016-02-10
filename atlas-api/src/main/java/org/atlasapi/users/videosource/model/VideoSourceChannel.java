package org.atlasapi.users.videosource.model;

import com.google.common.base.Preconditions;

public class VideoSourceChannel {

    private final String id;
    private final String title;
    private final String imageUrl;

    private VideoSourceChannel(String id, String title, String imageUrl) {
        this.id = id;
        this.title = title;
        this.imageUrl = imageUrl;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private String title;
        private String imageUrl;

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withImageUrl(String imageUrl) {
            this.imageUrl = imageUrl;
            return this;
        }

        public VideoSourceChannel build() {
            Preconditions.checkNotNull(id);
            Preconditions.checkNotNull(title);
            Preconditions.checkNotNull(imageUrl);
            return new VideoSourceChannel(id, title, imageUrl);
        }
    }

}
