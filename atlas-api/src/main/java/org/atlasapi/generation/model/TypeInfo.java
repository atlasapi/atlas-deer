package org.atlasapi.generation.model;

/**
 * Intermediate form to store parsed type information before outputting it to a generated source
 * file
 *
 * @author Oliver Hall (oli@metabroadcast.com)
 */
public interface TypeInfo {

    String className();

    String fullPackage();
}
