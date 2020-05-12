package com.poly.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;

public class S3 implements Serializable {

    private String s3bucket;
    private String s3Key;
    private static final Logger log = LogManager.getLogger(com.poly.utils.S3.class);
    private StringBuilder stringBuilder;
    private String awsregion;


    public S3(String s3bucket, String s3Key, StringBuilder stringBuilder, String awsregion) {
        this.s3bucket = s3bucket;
        this.s3Key = s3Key;
        this.stringBuilder = stringBuilder;
        this.awsregion = awsregion;

    }

    private ClientConfiguration cc = new ClientConfiguration()
            .withMaxConnections(Runtime.getRuntime().availableProcessors() - 1)
            .withMaxErrorRetry(10)
            .withConnectionTimeout(10000)
            .withSocketTimeout(10000)
            .withTcpKeepAlive(true);

    public void removeOjects() {
        AmazonS3 cli = AmazonS3ClientBuilder
                .standard()
                .withRegion(awsregion)
                .withClientConfiguration(cc)
                .withCredentials(new DefaultAWSCredentialsProviderChain()).build();
        try {
            S3Objects.withPrefix(cli, s3bucket, s3Key).forEach(
                    lsum -> {
                        System.out.println("remove PII object: " + lsum.getKey());
                        stringBuilder.append("remove PII object: " + lsum.getKey()).append("\n");
                        try {
                            cli.deleteObject(new DeleteObjectRequest(s3bucket, lsum.getKey()));

                        } catch (Exception e) {
                            stringBuilder.append(e.getCause()).append("\n");
                            log.error(e.getMessage());
                            log.error(e.getCause());
                            log.error(e.getStackTrace());
                        }
                    }
            );
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(e.getCause());
            log.error(e.getStackTrace());

        }

    }

    public void removeOjectsDelete(String exclude) {
        AmazonS3 cli = AmazonS3ClientBuilder
                .standard()
                .withRegion(awsregion)
                .withClientConfiguration(cc)
                .withCredentials(new DefaultAWSCredentialsProviderChain()).build();
        try {
            S3Objects.withPrefix(cli, s3bucket, s3Key).forEach(
                    lsum -> {
                        try {
                            if (!lsum.getKey().contains(exclude)) {
                                System.out.println("remove PII object: " + lsum.getKey());
                                stringBuilder.append("remove PII object: " + lsum.getKey()).append("\n");
                                cli.deleteObject(new DeleteObjectRequest(s3bucket, lsum.getKey()));
                            }
                        } catch (Exception e) {
                            stringBuilder.append(e.getCause()).append("\n");
                            log.error(e.getMessage());
                            log.error(e.getCause());
                            log.error(e.getStackTrace());
                        }
                    }
            );
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(e.getCause());
            log.error(e.getStackTrace());

        }

    }
}
