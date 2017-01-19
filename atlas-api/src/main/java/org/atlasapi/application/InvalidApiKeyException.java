package org.atlasapi.application;

public class InvalidApiKeyException extends Exception {

    private static final long serialVersionUID = -8204400513571208163L;
    private final String apiKey;
    private final String message;

    private InvalidApiKeyException(String apiKey, String message) {
        this.apiKey = apiKey;
        this.message = message;
    }

    public static InvalidApiKeyException create(String apiKey, String message) {
        return new InvalidApiKeyException(apiKey, message);
    }

    @Override
    public String getMessage() {
        return message + ": " + apiKey;
    }
}
