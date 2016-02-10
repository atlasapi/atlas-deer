package org.atlasapi.application.notification;

import java.util.Properties;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

public class JavaMailSenderFactory implements FactoryBean<JavaMailSender> {

    private final JavaMailSenderImpl sender;

    public JavaMailSenderFactory() {
        sender = new JavaMailSenderImpl();
        Properties properties = new Properties();
        properties.setProperty("mail.smtp.starttls.enable", "true");
        properties.setProperty("mail.smtp.auth", "true");
        properties.setProperty("mail.smtp.localhost", "localhost");
        sender.setJavaMailProperties(properties);
    }

    @Override
    public JavaMailSender getObject() throws Exception {
        return sender;
    }

    @Override
    public Class<?> getObjectType() {
        return JavaMailSender.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setHost(String host) {
        sender.setHost(host);
    }

    public void setUsername(String username) {
        sender.setUsername(username);
    }

    public void setPassword(String password) {
        sender.setPassword(password);
    }
}