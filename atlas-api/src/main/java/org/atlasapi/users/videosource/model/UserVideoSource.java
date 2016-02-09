package org.atlasapi.users.videosource.model;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.collect.ImmutableSet;

public class UserVideoSource {

    private final UserRef userRef;
    private final Id atlasUser;
    private final String name;
    private final Iterable<String> channelIds;
    private final Publisher publisher;
    private final OauthToken token;

    private UserVideoSource(UserRef userRef, Id atlasUser, String name,
            Iterable<String> channelIds, Publisher publisher, OauthToken token) {
        this.userRef = userRef;
        this.atlasUser = atlasUser;
        this.name = name;
        this.channelIds = ImmutableSet.copyOf(channelIds);
        this.publisher = publisher;
        this.token = token;
    }

    public UserRef getUserRef() {
        return userRef;
    }

    public Id getAtlasUser() {
        return atlasUser;
    }

    public String getName() {
        return name;
    }

    public Iterable<String> getChannelIds() {
        return channelIds;
    }

    public Publisher getPublisher() {
        return publisher;
    }

    public OauthToken getToken() {
        return token;
    }

    public Builder copy() {
        return builder()
                .withUserRef(this.getUserRef())
                .withAtlasUser(this.getAtlasUser())
                .withName(this.getName())
                .withChannelIds(this.getChannelIds())
                .withPublisher(this.getPublisher())
                .withToken(this.getToken());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private UserRef userRef;
        private Id atlasUser;
        private String name;
        private Iterable<String> channelIds = ImmutableSet.<String>of();
        private Publisher publisher;
        private OauthToken token;

        public Builder withUserRef(UserRef userRef) {
            this.userRef = userRef;
            return this;
        }

        public Builder withAtlasUser(Id atlasUser) {
            this.atlasUser = atlasUser;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withChannelIds(Iterable<String> channelIds) {
            this.channelIds = channelIds;
            return this;
        }

        public Builder addChannelId(String channelId) {
            ImmutableSet.Builder<String> channelIdsCopy = ImmutableSet.builder();
            channelIdsCopy.addAll(channelIds);
            channelIdsCopy.add(channelId);
            this.channelIds = channelIdsCopy.build();
            return this;
        }

        public Builder withPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withToken(OauthToken token) {
            this.token = token;
            return this;
        }

        public UserVideoSource build() {
            return new UserVideoSource(userRef, atlasUser, name,
                    channelIds, publisher, token
            );
        }
    }
}