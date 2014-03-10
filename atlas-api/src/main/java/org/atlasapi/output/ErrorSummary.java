package org.atlasapi.output;

import java.util.Map;
import java.util.UUID;

import org.atlasapi.application.auth.ApiKeyNotFoundException;
import org.atlasapi.application.auth.RevokedApiKeyException;
import org.atlasapi.query.common.InvalidAnnotationException;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser.MalformedDateTimeException;

public class ErrorSummary {
	
    public static interface ErrorSummaryFactory<T extends Exception> {
        
        ErrorSummary build(T exception);
        
    }
    
    public static final class DefaultErrorSummaryFactory implements ErrorSummaryFactory<Exception> {

        private final String errorCode;
        private final HttpStatusCode statusCode;

        public DefaultErrorSummaryFactory(String errorCode, HttpStatusCode statusCode) {
            this.errorCode = errorCode;
            this.statusCode = statusCode;
        }

        @Override
        public ErrorSummary build(Exception exception) {
            return new ErrorSummary(exception, errorCode, statusCode, exception.getMessage());
        }
        
    }
	
	private static Map<Class<? extends Exception>, ErrorSummaryFactory<?>> factories = factoryMap();
	
	public static <T extends Exception> ErrorSummary forException(T exception) {
        @SuppressWarnings("unchecked")
        ErrorSummaryFactory<? super T> factory = (ErrorSummaryFactory<? super T>) factories.get(exception.getClass());
		if (factory != null) {
			return factory.build(exception);
		} else {
			return new ErrorSummary(exception, "INTERNAL_ERROR", HttpStatusCode.SERVER_ERROR, "An internal server error occurred");
		}
	}
	
	private static Map<Class<? extends Exception>, ErrorSummaryFactory<?>> factoryMap() {
	    
		return ImmutableMap.<Class<? extends Exception>, ErrorSummaryFactory<?>>builder()
		    .put(IllegalArgumentException.class, new DefaultErrorSummaryFactory("BAD_QUERY_ATTRIBUTE", HttpStatusCode.BAD_REQUEST))
		    .put(MalformedDateTimeException.class, new DefaultErrorSummaryFactory("BAD_DATE_TIME_VALUE", HttpStatusCode.BAD_REQUEST))
		    .put(NotFoundException.class, new DefaultErrorSummaryFactory("RESOURCE_NOT_FOUND", HttpStatusCode.NOT_FOUND))
		    .put(NotAcceptableException.class, new DefaultErrorSummaryFactory("NOT_ACCEPTABLE", HttpStatusCode.NOT_ACCEPTABLE))
		    .put(InvalidAnnotationException.class, new DefaultErrorSummaryFactory("BAD_ANNOTATION_VALUE", HttpStatusCode.BAD_REQUEST))
		    // Sent when oauth tokens are not present. Request is invalid as missing params. Compatible with IE.
		    .put(NotAuthenticatedException.class, new DefaultErrorSummaryFactory("NOT_AUTHENTICATED", HttpStatusCode.BAD_REQUEST))
		    // Sent when logged in user does not have permission.
		    .put(NotAuthorizedException.class, new DefaultErrorSummaryFactory("UNAUTHORIZED", HttpStatusCode.FORBIDDEN))
		    .put(UserProfileIncompleteException.class, new DefaultErrorSummaryFactory("FORBIDDEN_USER_INCOMPLETE", HttpStatusCode.FORBIDDEN))
		    .put(ResourceForbiddenException.class, new DefaultErrorSummaryFactory("FORBIDDEN_RESOURCE", HttpStatusCode.FORBIDDEN))
		    .put(RevokedApiKeyException.class, new DefaultErrorSummaryFactory("REVOKED_API_KEY", HttpStatusCode.FORBIDDEN))
		    .put(ApiKeyNotFoundException.class, new DefaultErrorSummaryFactory("API_KEY_NOT_FOUND", HttpStatusCode.FORBIDDEN))
		    .put(LicenseNotAcceptedException.class, new DefaultErrorSummaryFactory("LICENSE_NOT_ACCEPTED", HttpStatusCode.FORBIDDEN))           
		    .put(InvalidTransitionException.class, new DefaultErrorSummaryFactory("INVALID_TRANSITION", HttpStatusCode.BAD_REQUEST))  
		    .build();
	}

	private final String id;
	private final Exception exception;
	private final String errorCode;
	private final HttpStatusCode statusCode;
	private final String message;
	
	public ErrorSummary(Exception exception, String errorCode, HttpStatusCode status, String msg) {
	    this.id = UUID.randomUUID().toString();
	    this.exception = exception;
	    this.errorCode = errorCode;
        this.statusCode = status;
		this.message = msg;
	}
	
	public String id() {
		return id;
	}
	
	public Exception exception() {
		return exception;
	}

	public HttpStatusCode statusCode() {
		return statusCode;
	}
	
	public String errorCode() {
		return errorCode;
	}
	
	public String message() {
		return this.message;
	}
}
