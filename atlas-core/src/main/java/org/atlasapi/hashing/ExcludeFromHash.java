package org.atlasapi.hashing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.annotation.meta.TypeQualifierDefault;

/**
 * This annotation indicates that a field should be excluded by our custom hashing logic
 */
@Documented
@TypeQualifierDefault(
        {
                ElementType.FIELD
        })
@Retention(RetentionPolicy.RUNTIME)
public @interface ExcludeFromHash {

}
