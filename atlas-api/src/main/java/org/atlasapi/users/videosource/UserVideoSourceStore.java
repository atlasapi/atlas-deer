package org.atlasapi.users.videosource;

import org.atlasapi.application.users.User;
import org.atlasapi.users.videosource.model.UserVideoSource;

import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

public interface UserVideoSourceStore {

    UserVideoSource sourceForRemoteuserRef(UserRef userRef);

    Iterable<UserVideoSource> userVideoSourcesFor(User user);

    Iterable<UserVideoSource> userVideoSourcesFor(User user, UserNamespace videoService);

    void store(UserVideoSource userVideoSource);
}
