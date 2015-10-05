package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
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

    /**
     * Update equivalence sets as represented by the changes in the provided update.
     * @param update
     * @throws WriteException
     */
    void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException;
    
    /**
     * Update piece of content.
     * @param content - reference to the resource which has changed.
     * @throws WriteException - if the update fails.
     */
    void updateContent(Content content) throws WriteException;


    /**
     * Resolves whole equivalejnt set
     * @param equivalentSetId
     * @return
     */
    ListenableFuture<Set<Content>> resolveEquivalentSet(Long equivalentSetId);
    
}
