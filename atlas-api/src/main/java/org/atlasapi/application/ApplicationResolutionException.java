package org.atlasapi.application;

public class ApplicationResolutionException extends Exception {

    private static final long serialVersionUID = -8204400513571208163L;
    private final String apiKey;
    private final String message;

    private ApplicationResolutionException(String apiKey, String message) {
        this.apiKey = apiKey;
        this.message = message;
    }

    public static ApplicationResolutionException create(String apiKey, String message) {
        return new ApplicationResolutionException(apiKey, message);
    }

    @Override
    public String getMessage() {
        return String.format("%s : %s", message, apiKey);
    }

}
