package org.atlasapi.entity;

import org.atlasapi.meta.annotations.FieldName;

public interface Identifiable {

	@FieldName("id")
    Id getId();

}