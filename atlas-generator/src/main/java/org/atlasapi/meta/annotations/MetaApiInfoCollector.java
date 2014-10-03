package org.atlasapi.meta.annotations;

import java.util.Map;

import javax.lang.model.element.Name;

import org.atlasapi.meta.annotations.model.Operation;
import org.atlasapi.meta.annotations.model.FieldInfo;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class MetaApiInfoCollector {

	private final Map<String, Name> typeMapping = Maps.newHashMap();
	private final Multimap<String, Operation> operationMapping = HashMultimap.create();
	private final Multimap<Name, FieldInfo> fieldMapping = HashMultimap.create();
	
	public MetaApiInfoCollector() {
	}
	
	public void setType(String key, Name type) {
		typeMapping.put(key, type);
	}
	
	public void setOperation(String key, Operation operation) {
		operationMapping.put(key, operation);
	}
	
	public void setField(Name type, FieldInfo field) {
		fieldMapping.put(type, field);
	}

	public Map<String, Name> getTypeMapping() {
		return typeMapping;
	}

	public Multimap<String, Operation> getOperationMapping() {
		return operationMapping;
	}

	public Multimap<Name, FieldInfo> getFieldMapping() {
		return fieldMapping;
	}
}
