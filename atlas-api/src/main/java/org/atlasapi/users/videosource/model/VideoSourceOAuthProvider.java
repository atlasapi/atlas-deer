package org.atlasapi.users.videosource.model;

import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.collect.ImmutableSet;

public enum VideoSourceOAuthProvider {
    YOUTUBE(
            UserNamespace.YOUTUBE,
            "Link your YouTube account",
            "/4/videosource/youtube/login",
            "youtube.png"
    );

    private final UserNamespace namespace;
    private final String loginPromptMessage;
    private final String authRequestUrl;
    private final String image;
    private static final ImmutableSet<VideoSourceOAuthProvider> ALL = ImmutableSet.copyOf(values());

    VideoSourceOAuthProvider(UserNamespace namespace, String loginPromptMessage,
            String authRequestUrl, String image) {
        this.namespace = namespace;
        this.loginPromptMessage = loginPromptMessage;
        this.authRequestUrl = authRequestUrl;
        this.image = image;
    }

    public UserNamespace getNamespace() {
        return namespace;
    }

    public String getLoginPromptMessage() {
        return loginPromptMessage;
    }

    public String getAuthRequestUrl() {
        return authRequestUrl;
    }

    public String getImage() {
        return image;
    }

    public static final ImmutableSet<VideoSourceOAuthProvider> all() {
        return ALL;
    }
}
