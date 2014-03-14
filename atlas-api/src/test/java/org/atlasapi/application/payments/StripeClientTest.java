package org.atlasapi.application.payments;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.junit.Test;

import com.metabroadcast.common.properties.Configurer;
import com.stripe.exception.APIConnectionException;
import com.stripe.exception.APIException;
import com.stripe.exception.AuthenticationException;
import com.stripe.exception.CardException;
import com.stripe.exception.InvalidRequestException;
import com.stripe.model.Customer;


public class StripeClientTest {
    private final String publishableKey = Configurer.get("stripe.publishable.key").get();
    private final String secretKey = Configurer.get("stripe.secret.key").get();
    
    @Test
    public void testCustomerCreation() throws AuthenticationException, InvalidRequestException, APIConnectionException, CardException, APIException {
        StripeClient client = new StripeClient(publishableKey, secretKey);
        Application application = Application.builder()
                .withCredentials(ApplicationCredentials.builder().withApiKey("testapikey").build())
                .build();
        User user = User.builder().withId(Id.valueOf(1)).withEmail("test@example.com").build();
        Customer customer = client.createCustomer(user, application);
        assertEquals(user.getEmail(), customer.getEmail());
        assertEquals(application.getCredentials().getApiKey(), customer.getDescription());
        // customer.getId()
        application = application.copy().withStripeCustomerId(customer.getId()).build();
        assertTrue(client.deleteCustomer(application));
    }

}
