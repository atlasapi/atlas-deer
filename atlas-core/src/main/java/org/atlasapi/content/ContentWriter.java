package org.atlasapi.content;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

public interface ContentWriter {

    <C extends Content> WriteResult<C, Content> writeContent(C content) throws WriteException;
    
}
