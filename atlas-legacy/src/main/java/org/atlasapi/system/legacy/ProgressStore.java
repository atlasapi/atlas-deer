package org.atlasapi.system.legacy;

import java.util.Optional;

import org.atlasapi.persistence.content.listing.ContentListingProgress;

public interface ProgressStore {

    Optional<ContentListingProgress> progressForTask(String taskName);

    void storeProgress(String taskName, ContentListingProgress progress);

}
