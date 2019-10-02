package org.atlasapi.entity;

import javax.annotation.Nullable;

public interface Sameable {

    /**
     * An equals implementation to be used when determining if two objects are essentially the same. This will be used
     * by migration to determine whether a database write is required.
     * @param other the object to compare to
     * @return true if the two objects are deemed to be essentially the same
     */
    boolean isSame(@Nullable Sameable other);

}