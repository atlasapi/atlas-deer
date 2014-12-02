package org.atlasapi.generation.output.writers;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;

import org.atlasapi.generation.model.TypeInfo;

public interface SourceFileWriter<TI extends TypeInfo> {
	
	
	void init(Filer filer, Messager messager);

	/**
	 * Generates a file from a given type and writes the provided source 
	 * @param type The source type from which the output file is being generated
	 * @param typeSource A string containing the full source of file to be output
	 */
	void writeFile(TI typeInfo, String typeSource);
}
