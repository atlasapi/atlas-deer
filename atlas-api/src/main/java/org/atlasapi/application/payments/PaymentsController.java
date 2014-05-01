package org.atlasapi.application.payments;


import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.users.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;



@Controller
public class PaymentsController {
    private static Logger log = LoggerFactory.getLogger(PaymentsController.class);
    private final StripeClient stripeClient;
    private final UserFetcher userFetcher;

    public PaymentsController(StripeClient stripeClient, UserFetcher userFetcher) {
        this.stripeClient = checkNotNull(stripeClient);
        this.userFetcher = checkNotNull(userFetcher);
    }

    // TODO Add in endpoints to subscribe to source in MBST-8448

}
