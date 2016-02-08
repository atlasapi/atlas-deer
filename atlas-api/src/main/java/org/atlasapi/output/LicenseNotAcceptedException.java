package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

public class LicenseNotAcceptedException extends QueryExecutionException {

    private static final long serialVersionUID = 7886280626313023470L;

    public LicenseNotAcceptedException() {
        super("License terms must be accepted");
    }
}
