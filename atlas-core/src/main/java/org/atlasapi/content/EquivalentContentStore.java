package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.EquivalentsResolver;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Store of equivalence sets of resources. 
 *
 */
//TODO Can this be EquivalentStore<T> in the future?
//TODO Given there's already EquivalentsResolver have EquivalentsWriter too as super-interfaces?
public interface EquivalentContentStore extends EquivalentsResolver<Content> {

    void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException;
    
    void updateContent(Id contentId) throws WriteException;

    ListenableFuture<Set<Content>> resolveEquivalentSet(Long equivalentSetId);
}
