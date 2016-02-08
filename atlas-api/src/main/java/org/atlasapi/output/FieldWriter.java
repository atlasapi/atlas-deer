package org.atlasapi.output;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * <p>In a certain format, a FieldWriter writes a value in a particular named field either directly
 * or through an {@link EntityWriter} or {@link EntityListWriter}.</p>
 */
public interface FieldWriter {

    /**
     * <p>Write a possibly-null value to a to a named field.</p> <p>If the value is null the field
     * may be ignored or written with a null value representation.<p>
     *
     * @param field - the field name
     * @param obj   - the value
     * @throws IOException if the field cannot be written
     */
    void writeField(String field, @Nullable Object obj) throws IOException;

    /**
     * <p>Writes a possibly-null value through the provided {@link EntityWriter} according to the
     * given {@link OutputContext}.</p>
     * <p>
     * <p>The name of the field is derived using the writer and value via {@link
     * EntityWriter#fieldName}</p>
     *
     * @param writer - the writer to use to write the value
     * @param obj    - the value to write
     * @param ctxt   - the context of the write
     * @throws IOException if the field cannot be written
     */
    <T> void writeObject(EntityWriter<? super T> writer, @Nullable T obj, OutputContext ctxt)
            throws IOException;

    /**
     * <p>Behaves as {@link #writeObject(EntityWriter, T, OutputContext)} but the field name is
     * provided rather than derived from the value.</p>
     *
     * @param writer    - the writer to use to write the value
     * @param fieldName - the field name to use
     * @param obj       - the value to write
     * @param ctxt      - the context of the write
     * @throws IOException if the field cannot be written
     */
    <T> void writeObject(EntityWriter<? super T> writer, @Nonnull String fieldName,
            @Nullable T obj, OutputContext ctxt)
            throws IOException;

    /**
     * <p>Writes a list of values to a single field.</p>
     *
     * @param field - the field name to use
     * @param elem  - the element field name to use
     * @param list  - the values to write
     * @param ctxt  - the context of the write
     * @throws IOException if the field cannot be written
     */
    void writeList(String field, String elem, Iterable<?> list, OutputContext ctxt)
            throws IOException;

    /**
     * <p>Writes a list of values to a single field through an {@link EntityListWriter}.</p>
     * <p>
     * <p>The field name and element field names are derived via {@link EntityListWriter#listName()}
     * and {@link EntityListWriter#fieldName(Object)} respectfully.</p>
     *
     * @param listWriter - the writer to use to write the list of values
     * @param list       - the list of values
     * @param ctxt       - the context of the write
     * @throws IOException if the field cannot be written
     */
    <T> void writeList(EntityListWriter<? super T> listWriter,
            Iterable<T> list, OutputContext ctxt) throws IOException;

}