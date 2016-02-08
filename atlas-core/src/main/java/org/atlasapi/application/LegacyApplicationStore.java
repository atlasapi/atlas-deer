package org.atlasapi.application;

import org.atlasapi.entity.Id;

import com.google.common.base.Optional;

public interface LegacyApplicationStore extends ApplicationStore {

    /**
     * Compatibility method for Atlas 3.0 datasets
     *
     * @param slug
     * @return Application for slug
     */
    @Deprecated
    Optional<Id> applicationIdForSlug(String slug);

    /**
     * Compatibility method for Atlas 3.0 datasets
     *
     * @param slug
     * @return Applications for slugs
     */
    @Deprecated
    Iterable<Id> applicationIdsForSlugs(Iterable<String> slugs);
}
