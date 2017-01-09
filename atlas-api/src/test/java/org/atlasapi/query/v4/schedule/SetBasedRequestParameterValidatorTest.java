package org.atlasapi.query.v4.schedule;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.common.exceptions.InvalidParameterException;
import org.atlasapi.query.common.validation.SetBasedRequestParameterValidator;

import com.metabroadcast.common.servlet.StubHttpServletRequest;

import org.junit.Test;

public class SetBasedRequestParameterValidatorTest {

    SetBasedRequestParameterValidator validator = SetBasedRequestParameterValidator.builder()
            .withRequiredParameters("from", "to", "publisher")
            .withOptionalParameters("apiKey", "annotations")
            .build();

    @Test
    public void testValidatesParameters() throws Exception {
        validator.validateParameters(requestWithParams("from", "to", "publisher"));
        validator.validateParameters(requestWithParams("from",
                "to",
                "publisher",
                "apiKey",
                "annotations"));
    }

    @Test(expected = InvalidParameterException.class)
    public void testThrowsExceptionMissingParameters() throws Exception {
        validator.validateParameters(requestWithParams("from", "to"));
    }

    @Test(expected = InvalidParameterException.class)
    public void testThrowsExceptionInvalidParameters() throws Exception {
        validator.validateParameters(requestWithParams("form", "to", "wibble"));
    }

    private HttpServletRequest requestWithParams(String... params) {
        StubHttpServletRequest request = new StubHttpServletRequest();
        for (String param : params) {
            request.withParam(param, param);
        }
        return request;
    }

}
