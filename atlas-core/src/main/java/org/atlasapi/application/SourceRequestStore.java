package org.atlasapi.application;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.IdResolver;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;

/**
 * Source Requests are a record of when a user asks to gain access to a restricted source. This
 * interface specifies how they should be stored.
 *
 * @author liam
 */
public interface SourceRequestStore extends IdResolver<SourceRequest> {

    /**
     * Save source request to store. SourceRequest should have an ID set.
     *
     * @param sourceRequest
     */
    void store(SourceRequest sourceRequest);

    /**
     * Gets a matching source request for an application and source.
     *
     * @param applicationId
     * @param source
     * @return Existing source request if found
     */
    Optional<SourceRequest> getBy(Id applicationId, Publisher source);

    /**
     * Returns all source requests for a publisher
     *
     * @param source
     * @return
     */
    Set<SourceRequest> sourceRequestsFor(Publisher source);

    /**
     * Returns all stored source requests
     *
     * @return
     */
    Set<SourceRequest> all();

    /**
     * Returns a source request for a given id
     *
     * @param id
     * @return Source request if it exists
     */
    Optional<SourceRequest> sourceRequestFor(Id id);

    /**
     * Returns source requests for given applications
     */
    Iterable<SourceRequest> sourceRequestsForApplicationIds(Iterable<Id> applicationIds);

}
