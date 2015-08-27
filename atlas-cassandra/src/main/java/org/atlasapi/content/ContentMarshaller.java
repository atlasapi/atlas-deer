package org.atlasapi.content;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.ColumnList;

public interface ContentMarshaller {

    void marshallInto(ColumnListMutation<String> mutation, Content content);

    Content unmarshallCols(ColumnList<String> columns);

}