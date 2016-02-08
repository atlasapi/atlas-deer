package org.atlasapi.output;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ErrorResult {

    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final ErrorSummary errorSummary;

    public ErrorResult(HttpServletRequest request, HttpServletResponse response,
            ErrorSummary errorSummary) {
        this.request = request;
        this.response = response;
        this.errorSummary = errorSummary;
    }

    public HttpServletRequest getRequest() {
        return this.request;
    }

    public HttpServletResponse getResponse() {
        return this.response;
    }

    public ErrorSummary getErrorSummary() {
        return this.errorSummary;
    }

}
