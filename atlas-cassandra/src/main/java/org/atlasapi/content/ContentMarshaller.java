package org.atlasapi.content;

import java.util.Optional;

import org.atlasapi.entity.Id;

public interface ContentMarshaller<M, U> {

    void marshallInto(
            Id id,
            M columnBatch,
            Content content,
            Optional<Content> previousContent,
            Boolean setEmptyRepeatedFieldsToNull
    );

    Content unmarshallCols(U columns);
}
