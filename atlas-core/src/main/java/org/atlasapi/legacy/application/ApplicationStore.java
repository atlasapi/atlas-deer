package org.atlasapi.legacy.application;

import java.util.Set;

import org.atlasapi.legacy.user.User;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;

public interface ApplicationStore {

    Iterable<Application> allApplications();

    Set<Application> applicationsFor(Optional<User> user);

    Set<Application> applicationsFor(Publisher source);

    Optional<Application> applicationFor(String slug);

    Optional<Application> applicationForKey(String key);

    Application persist(Application application);

    Application update(Application application);

}