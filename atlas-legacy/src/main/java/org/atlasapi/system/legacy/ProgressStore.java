package org.atlasapi.system.legacy;

import org.atlasapi.persistence.content.listing.ContentListingProgress;

import java.util.Optional;

public interface ProgressStore {

    Optional<ContentListingProgress> progressForTask(String taskName);
    void storeProgress(String taskName, ContentListingProgress progress);

}
