package org.atlasapi.application.notification;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.webapp.soy.SoyTemplateRenderer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;

@Configuration
public class NotifierModule {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private @Value("${notifications.email.host}") String emailHost;
    private @Value("${notifications.email.username}") String emailUsername;
    private @Value("${notifications.email.password}") String emailPassword;
    private @Value("${notifications.email.from}") String from;
    private @Value("${notifications.email.fromFriendlyName}") String fromFriendlyName;
    private @Value("${notifications.email.to}") String to;

    @Bean
    public EmailNotificationSender emailSender() {
        JavaMailSender sender = createSender();
        return EmailNotificationSender.builder()
                .withMailSender(sender)
                .withRenderer(soyRenderer())
                .withIdCodec(idCodec)
                .withAdminToField(to)
                .withFromField(from)
                .withFriendlyFromName(fromFriendlyName)
                .build();
    }

    private JavaMailSender createSender() {
        try {
            JavaMailSenderFactory factory = new JavaMailSenderFactory();
            factory.setHost(emailHost);
            factory.setUsername(emailUsername);
            factory.setPassword(emailPassword);
            return factory.getObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public SoyTemplateRenderer soyRenderer() {
        SoyTemplateRenderer renderer = new SoyTemplateRenderer();
        renderer.setPrefix("/WEB-INF/templates/");
        renderer.setSuffix(".soy");
        return renderer;
    }

}
