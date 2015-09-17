package org.atlasapi.content;


import org.atlasapi.entity.Id;

public interface ContentMarshaller<M,U> {

    void marshallInto(Id id, M columnBatch, Content content, Boolean setEmptyRepeatedFieldsToNull);

    Content unmarshallCols(U columns);

}