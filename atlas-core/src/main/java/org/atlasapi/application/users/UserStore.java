package org.atlasapi.application.users;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdResolver;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;

public interface UserStore extends IdResolver<User> {

    Optional<User> userForRef(UserRef ref);

    Optional<User> userForId(Id id);

    Optional<User> userForEmail(String email);

    Iterable<User> allUsers();

    void store(User user);

}
