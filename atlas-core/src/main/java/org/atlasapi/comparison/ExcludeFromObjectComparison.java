package org.atlasapi.comparison;

import javax.annotation.meta.TypeQualifierDefault;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation indicates that a field should be excluded from our custom {@link ObjectComparer} logic
 */
@Documented
@TypeQualifierDefault(
        {
                ElementType.FIELD
        })
@Retention(RetentionPolicy.RUNTIME)
public @interface ExcludeFromObjectComparison {

}
