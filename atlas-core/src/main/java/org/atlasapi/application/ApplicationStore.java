package org.atlasapi.application;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;

/**
 * Applications connect a configuration to an API key
 */
public interface ApplicationStore extends IdResolver<Application> {

    Iterable<Application> allApplications();

    /**
     * Returns application for a specific ID if it exists
     *
     * @param id
     * @return Application object if found
     */
    Optional<Application> applicationFor(Id id);

    /**
     * Add a new application to store. Implementations of this method should ensure that the
     * application gets an ID, slug, API key and created time.
     *
     * @param application
     * @return Application with newly allocated ID
     */
    Application createApplication(Application application);

    /**
     * Replaces stored information for an application
     *
     * @param application
     * @return Application object
     */
    Application updateApplication(Application application);

    /**
     * Returns all applications that can read data published under this source
     *
     * @param source
     * @return Applications with read access to this source
     */
    Iterable<Application> readersFor(Publisher source);

    /**
     * Returns all applications that can add data as this source
     *
     * @param source
     * @return Applications with write access to this source
     */
    Iterable<Application> writersFor(Publisher source);

    /**
     * Returns an application for an api key passed as part of the request if one exists
     *
     * @param apiKey String of api key
     * @return Application if found
     */
    Optional<Application> applicationForKey(String apiKey);
}
