package org.atlasapi.generation.output;

import org.atlasapi.generation.model.MethodInfo;
import org.atlasapi.generation.model.TypeInfo;

public interface SourceGenerator<TI extends TypeInfo, MI extends MethodInfo> {

    String processType(TI typeInfo, Iterable<MI> methodsInfo);
}
