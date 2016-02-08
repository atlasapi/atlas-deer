package org.atlasapi.output;

import java.io.IOException;

/**
 * <p>A {@link FieldWriter} that also is able to start and end an complete response.</p>
 */
public interface ResponseWriter extends FieldWriter {

    /**
     * <p>Start the response and write any required pre-amble.</p>
     *
     * @throws IOException
     */
    void startResponse() throws IOException;

    /**
     * <p>Complete the response and write any required post-amble.</p>
     *
     * @throws IOException
     */
    void finishResponse() throws IOException;

}
