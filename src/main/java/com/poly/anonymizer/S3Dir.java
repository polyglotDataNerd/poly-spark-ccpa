package com.poly.anonymizer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public class S3Dir implements Runnable, Serializable {

    private String s3bucket;
    private String s3Key;
    private ConcurrentLinkedDeque<Map<String, byte[]>> lbqFileMap;
    private static final Logger log = LogManager.getLogger(S3Dir.class);
    private StringBuilder stringBuilder;


    public S3Dir(String s3bucket, String s3Key, ConcurrentLinkedDeque<Map<String, byte[]>> lbqFileMap, StringBuilder stringBuilder) {
        this.s3bucket = s3bucket;
        this.s3Key = s3Key;
        this.lbqFileMap = lbqFileMap;
        this.stringBuilder = stringBuilder;

    }

    private ClientConfiguration cc = new ClientConfiguration()
            .withMaxConnections(Runtime.getRuntime().availableProcessors() - 1)
            .withMaxErrorRetry(10)
            .withConnectionTimeout(10000)
            .withSocketTimeout(10000)
            .withTcpKeepAlive(true);

    private AmazonS3 cli = AmazonS3ClientBuilder
            .standard()
            .withRegion(Regions.US_WEST_2)
            .withClientConfiguration(cc)
            .withCredentials(new DefaultAWSCredentialsProviderChain()).build();
    private ListObjectsRequest obj = new ListObjectsRequest();

    @Override
    public void run() {
        try {
            getOjects();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
        }
    }

    private void getOjects() {
        try {
            S3Objects.withPrefix(cli, s3bucket, s3Key).forEach(
                    lsum -> {
                        try {
                            byte[] payload = IOUtils.toByteArray(cli.getObject(new GetObjectRequest(s3bucket, lsum.getKey())).getObjectContent());
                            if (payload.length != 0) {
                                String key = lsum.getKey();
                                System.out.println("producer thread: " + Thread.currentThread().getName() + "->" + key);
                                stringBuilder.append("INFO producer thread: " + Thread.currentThread().getName() + "->" + key).append("\n");
                                lbqFileMap.add(Collections.singletonMap(key, payload));
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
