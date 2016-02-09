package org.atlasapi.output;

import javax.annotation.Nonnull;

/**
 * <p>An {@link EntityWriter} which can also write an list of entities.</p>
 *
 * @param <T>
 */
public interface EntityListWriter<T> extends EntityWriter<T> {

    /**
     * <p>The name to use for the list field.</p>
     *
     * @return the name of the list field
     */
    @Nonnull
    String listName();

}
