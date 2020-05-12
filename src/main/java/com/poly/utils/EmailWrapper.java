package com.poly.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Arrays;


public class EmailWrapper implements Serializable {

    String toEmail;
    String fromEmail;
    String subject;
    String body;
    private static Logger log = LogManager.getLogger(EmailWrapper.class);

    public EmailWrapper(String toEmail, String fromEmail, String subject, String body) {
        this.toEmail = toEmail;
        this.fromEmail = fromEmail;
        this.subject = subject;
        this.body = body;

    }

    public void sendEMail() {
        AmazonSimpleEmailService client =
                AmazonSimpleEmailServiceClientBuilder.standard()
                        .withRegion(Regions.US_WEST_2)
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();
        SendEmailRequest request = new SendEmailRequest()
                .withDestination(
                        new Destination().withToAddresses(Arrays.asList(toEmail.split(","))))
                .withMessage(new Message()
                        .withBody(new Body()
                                .withText(new Content()
                                        .withCharset("UTF-8").withData(body)))
                        .withSubject(new Content()
                                .withCharset("UTF-8").withData(subject)))
                .withSource(fromEmail);
        client.sendEmail(request);
        log.info("Email sent to:" + toEmail);
    }
}
