package org.atlasapi.output;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * <p>An {@link EntityWriter} can write an entity out through a provided {@link FieldWriter} in a
 * given {@link OutputContext}.</p>
 *
 * @param <T> the type of entities this writer writes.
 */
public interface EntityWriter<T, R> {

    /**
     * <p>Write out the entity using the {@link FieldWriter} according to the {@link
     * OutputContext}.</p>
     *
     * @param entity - the entity to write
     * @param writer - the writer with which to write the entity
     * @param ctxt   - the context of the write
     * @throws IOException if the entity cannot be written
     */
    void write(@Nonnull R entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt)
            throws IOException;

    /**
     * <p>Determines the name of the field into which the provided entity will be written.</p>
     * <p>
     * <p>The name can be constant, derived from the given entity or determined by some other
     * means.</p>
     *
     * @param entity - the entity which will be written into this field.
     * @return the name to use for the field.
     */
    @Nonnull
    String fieldName(R entity);

}
