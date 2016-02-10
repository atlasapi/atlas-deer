package org.atlasapi.application.notification;

import java.io.UnsupportedEncodingException;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.atlasapi.application.Application;
import org.atlasapi.application.SourceRequest;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.model.SimpleModel;
import com.metabroadcast.common.webapp.soy.TemplateRenderer;

import com.google.common.base.Charsets;
import org.elasticsearch.common.Preconditions;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;

public class EmailNotificationSender {

    public static final Builder builder() {
        return new Builder();
    }

    public final static class Builder {

        private JavaMailSender sender;
        private TemplateRenderer renderer;

        private String from;
        private String fromFriendlyName;
        private String adminTo;
        private NumberToShortStringCodec idCodec;

        public Builder withMailSender(JavaMailSender sender) {
            this.sender = sender;
            return this;
        }

        public Builder withRenderer(TemplateRenderer renderer) {
            this.renderer = renderer;
            return this;
        }

        public Builder withIdCodec(NumberToShortStringCodec idCodec) {
            this.idCodec = idCodec;
            return this;
        }

        public Builder withAdminToField(String to) {
            this.adminTo = to;
            return this;
        }

        public Builder withFromField(String from) {
            this.from = from;
            return this;
        }

        public Builder withFriendlyFromName(String fromFriendlyName) {
            this.fromFriendlyName = fromFriendlyName;
            return this;
        }

        public EmailNotificationSender build() {
            Preconditions.checkNotNull(sender);
            Preconditions.checkNotNull(renderer);
            EmailNotificationSender emailNotificationSender = new EmailNotificationSender(
                    this.sender,
                    this.renderer,
                    this.from,
                    this.fromFriendlyName,
                    this.adminTo,
                    this.idCodec
            );
            return emailNotificationSender;
        }
    }

    private static final String ADMIN_NOTIFICATION_TEMPLATE = "atlas.templates.applications.admin.email.body";
    private static final String ADMIN_NOTIFICATION_SUBJECT_TEMPLATE = "atlas.templates.applications.admin.email.subject";
    private static final String USER_SUCCESS_NOTIFICATION_TEMPLATE = "atlas.templates.applications.user.success.email.body";
    private static final String USER_SUCCESS_NOTIFICATION_SUBJECT_TEMPLATE = "atlas.templates.applications.user.success.email.subject";

    private final JavaMailSender sender;
    private final TemplateRenderer renderer;

    private final String from;
    private final String fromFriendlyName;
    private final String adminTo;
    private final NumberToShortStringCodec idCodec;

    private EmailNotificationSender(JavaMailSender sender,
            TemplateRenderer renderer,
            String from,
            String fromFriendlyName,
            String adminTo,
            NumberToShortStringCodec idCodec) {
        this.sender = sender;
        this.renderer = renderer;
        this.from = from;
        this.fromFriendlyName = fromFriendlyName;
        this.adminTo = adminTo;
        this.idCodec = idCodec;
    }

    public void sendNotificationOfPublisherRequestToAdmin(Application app,
            SourceRequest sourceRequest) throws MessagingException, UnsupportedEncodingException {
        MimeMessage message = sender.createMimeMessage();

        MimeMessageHelper helper = new MimeMessageHelper(message, false, Charsets.UTF_8.name());
        SimpleModel model = new SimpleModel();
        model.put("publisher_key", sourceRequest.getSource().key());
        model.put("publisher_title", sourceRequest.getSource().title());
        model.put("usage_type", sourceRequest.getUsageType().title());
        model.put("email", sourceRequest.getEmail());
        model.put("appUrl", sourceRequest.getAppUrl());
        model.put("reason", sourceRequest.getReason());
        model.put("app_id", idCodec.encode(sourceRequest.getAppId().toBigInteger()));
        model.put("application_title", app.getTitle());
        helper.setTo(this.adminTo);
        helper.setFrom(this.from, this.fromFriendlyName);
        helper.setText(renderer.render(ADMIN_NOTIFICATION_TEMPLATE, model), true);
        helper.setSubject(renderer.render(ADMIN_NOTIFICATION_SUBJECT_TEMPLATE, model));

        sender.send(message);
    }

    public void sendNotificationOfPublisherRequestSuccessToUser(Application app,
            SourceRequest sourceRequest) throws MessagingException, UnsupportedEncodingException {
        MimeMessage message = sender.createMimeMessage();

        MimeMessageHelper helper = new MimeMessageHelper(message, false, Charsets.UTF_8.name());
        SimpleModel model = new SimpleModel();
        model.put("publisher_key", sourceRequest.getSource().key());
        model.put("publisher_title", sourceRequest.getSource().title());
        model.put("usage_type", sourceRequest.getUsageType().title());
        model.put("appUrl", sourceRequest.getAppUrl());
        model.put("reason", sourceRequest.getReason());
        model.put("app_id", idCodec.encode(sourceRequest.getAppId().toBigInteger()));
        model.put("application_title", app.getTitle());

        helper.setTo(sourceRequest.getEmail());
        helper.setFrom(this.from, this.fromFriendlyName);
        helper.setText(renderer.render(USER_SUCCESS_NOTIFICATION_TEMPLATE, model), true);
        helper.setSubject(renderer.render(USER_SUCCESS_NOTIFICATION_SUBJECT_TEMPLATE, model));
        sender.send(message);
    }
}