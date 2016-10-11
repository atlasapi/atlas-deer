/**
 * This package contains the logic for generating the API output. This code is structured
 * in the following way:
 *
 * <ul>
 *     <li>An implementation of {@link org.atlasapi.output.QueryResultWriter} is responsible
 *     for writing the top level of the output including information about the request query and
 *     the license. It also begins the top level object or list (depending on how many are being
 *     written) of the entity being output. This then delegates to</li>
 *     <li>an implementation of {@link org.atlasapi.output.EntityListWriter} which retrieves
 *     all the {@link org.atlasapi.output.annotation.OutputAnnotation} that are enabled
 *     for the request and delegates to them in sequence.</li>
 *     <li>Each {@link org.atlasapi.output.annotation.OutputAnnotation} then delegates to one
 *     or more instances of {@link org.atlasapi.output.EntityWriter} or
 *     {@link org.atlasapi.output.EntityListWriter} to write the data of the output. They can
 *     then delegate to other writers as required to generate nested data</li>
 * </ul>
 * <p>
 * With respect to the last two the intention is that:
 * <ul>
 *     <li>{@link org.atlasapi.output.annotation.OutputAnnotation} can be tailored to
 *     specific endpoints, i.e. even if the string of the annotation in the request parameter
 *     is the same across two different endpoints (e.g. "id" on /4/content and /4/channel) the
 *     class implementing the annotation for each endpoint can be different. This allows for
 *     customising the return type of each endpoint's
 *     {@link org.atlasapi.query.common.QueryExecutor} to include as much data as we wish
 *     without having to worry about maintaining consistency between different endpoints using
 *     the same annotations</li>
 *     <li>{@link org.atlasapi.output.EntityWriter} and {@link org.atlasapi.output.EntityListWriter}
 *     on the other hand should be as common as possible and reused between different
 *     implementations of the same annotation to ensure that where we are outputting the same data
 *     we are doing so in the same way</li>
 * </ul>
 */
@NonNullByDefault
package org.atlasapi.output;

import com.metabroadcast.common.annotation.NonNullByDefault;

