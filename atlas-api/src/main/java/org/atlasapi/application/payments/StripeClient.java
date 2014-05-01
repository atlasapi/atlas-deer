package org.atlasapi.application.payments;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stripe.Stripe;


public class StripeClient {
    private static Logger log = LoggerFactory.getLogger(StripeClient.class);
    private final String publishableKey;
    
    public StripeClient(String publishableKey, String secretKey) {
        this.publishableKey = checkNotNull(publishableKey);
        Stripe.apiKey = checkNotNull(secretKey);
    }
    
    // TODO Call endpoints in Stripe in MBST-8448
}
