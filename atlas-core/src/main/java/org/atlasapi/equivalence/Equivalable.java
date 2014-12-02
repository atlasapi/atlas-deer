package org.atlasapi.equivalence;

import java.util.Set;

import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.meta.annotations.FieldName;

@Deprecated
public interface Equivalable<E extends Equivalable<E>> extends Identifiable, Sourced {

	@FieldName("equivalent_to")
    Set<EquivalenceRef> getEquivalentTo();
    
    //I don't like this method.
    E copyWithEquivalentTo(Iterable<EquivalenceRef> equivalents);
    
}
