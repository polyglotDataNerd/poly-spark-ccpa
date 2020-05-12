package com.poly.anonymizer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.poly.utils.CompressWrite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.zip.GZIPInputStream;


public class ObjectAnonymizer implements Runnable, Serializable {
    String type;
    ConcurrentLinkedDeque<Map<String, byte[]>> lbqFileMap;
    ArrayList<String> piiArray;
    StringBuilder stringBuilder;
    String s3target;


    public ObjectAnonymizer(String type, ConcurrentLinkedDeque<Map<String, byte[]>> lbqFileMap, ArrayList<String> piiArray, StringBuilder stringBuilder, String s3target) {
        this.type = type;
        this.lbqFileMap = lbqFileMap;
        this.piiArray = piiArray;
        this.stringBuilder = stringBuilder;
        this.s3target = s3target;
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

    @Override
    public void run() {

        if (type.contains("source1")) {
            while (true) {
                clickstream();
                if (lbqFileMap.isEmpty()) {
                    System.out.println("queue is Empty");
                    break;
                }
            }
        } else if (type.contains("source2")) {
            while (true) {
                chatbot();
                if (lbqFileMap.isEmpty()) {
                    System.out.println("queue is Empty");
                    stringBuilder.append("queue is Empty").append("\n");
                    break;
                }
            }
        } else if (type.contains("source2")) {
            while (true) {
                customerservice();
                if (lbqFileMap.isEmpty()) {
                    stringBuilder.append("queue is Empty").append("\n");
                    System.out.println("queue is Empty");
                    break;
                }
            }
        } else if (type.contains("source3")) {
            while (true) {
                events();
                if (lbqFileMap.isEmpty()) {
                    stringBuilder.append("queue is Empty").append("\n");
                    System.out.println("queue is Empty");
                    break;
                }
            }
        }
    }

    private void clickstream() {
        try {
            lbqFileMap
                    .poll()
                    .entrySet()
                    .stream()
                    .forEach(keypair -> {
                        System.out.println("consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey());
                        stringBuilder.append("INFO consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey()).append("\n");
                        try (JsonReader source = new JsonReader(new InputStreamReader(new ByteArrayInputStream(keypair.getValue())))) {
                            JsonObject payload = new Gson().fromJson(source, JsonObject.class);
                            JsonArray nestedJson = payload.getAsJsonArray("results");
                            nestedJson.forEach(y -> {
                                piiArray.forEach(anonymizedKey -> {
                                    y.getAsJsonObject().get("$properties").getAsJsonObject().addProperty(anonymizedKey, "");
                                });

                            });
                            try (ByteArrayInputStream bis = new ByteArrayInputStream(payload.toString().getBytes())) {
                                ObjectMetadata objectMetadata = new ObjectMetadata();
                                objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                                objectMetadata.setContentLength(payload.toString().length());
                                PutObjectRequest por = new PutObjectRequest(s3target, "anonymized/" + keypair.getKey(), bis, objectMetadata);
                                cli.putObject(por);
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                                stringBuilder.append(e.getMessage()).append("\n");
                            }
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            stringBuilder.append(e.getMessage()).append("\n");
                        }
                    });
        } catch (Exception e) {
            System.out.println(e.getMessage());
            stringBuilder.append(e.getMessage()).append("\n");
        }
    }

    private void chatbot() {
        String REPLACE = "(anonymized PII)";
        try {
            lbqFileMap
                    .poll()
                    .entrySet()
                    .stream()
                    .forEach(keypair -> {
                        /*since this is a spark job we want to skip the success file it generates*/
                        System.out.println("consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey());
                        stringBuilder.append("INFO consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey()).append("\n");
                        try (JsonReader source = new JsonReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(keypair.getValue()))))) {
                            JsonObject payload = new Gson().fromJson(source, JsonObject.class);
                            JsonArray parentNestedJson = payload.getAsJsonArray("yepchat");
                            parentNestedJson.forEach(mainarray -> {
                                JsonArray nestedJson = mainarray.getAsJsonObject().get("payload").getAsJsonObject().getAsJsonArray("conversations");
                                nestedJson.forEach(childArray -> {
                                    piiArray.forEach(anonymizedKey -> {
                                        /*anonymizes via json key array*/
                                        childArray.getAsJsonObject().get("guest").getAsJsonObject().addProperty(anonymizedKey, REPLACE);
                                    });
                                    childArray.getAsJsonObject().get("messages").getAsJsonArray().forEach(subarray -> {
                                        /*anonymizes text via regex pattern matching*/
                                        subarray.getAsJsonObject().addProperty("body", textMasker(subarray.getAsJsonObject().get("body").getAsString()));
                                    });
                                });
                            });
                            try (
                                    ByteArrayOutputStream compressRaw = new CompressWrite().writestreamGZIP(payload.toString());
                                    ByteArrayInputStream bios = new ByteArrayInputStream(compressRaw.toByteArray())
                            ) {
                                ObjectMetadata objectMetadata = new ObjectMetadata();
                                objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                                objectMetadata.setContentLength(compressRaw.size());
                                PutObjectRequest por = new PutObjectRequest(s3target, "anonymized/" + keypair.getKey(), bios, objectMetadata);
                                System.out.println(por.getBucketName());
                                System.out.println(por.getCustomRequestHeaders());
                                System.out.println(por.getKey());
                                System.out.println(por.getMetadata());
                                cli.putObject(por);
                            }

                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            stringBuilder.append(e.getMessage()).append("\n");
                        }


                    });
        } catch (Exception e) {
            System.out.println(e.getMessage());
            stringBuilder.append(e.getMessage()).append("\n");
        }
    }

    private void customerservice() {
        String REPLACE = "(anonymized PII)";
        try {
            lbqFileMap
                    .poll()
                    .entrySet()
                    .parallelStream()
                    .forEach(keypair -> {
                        System.out.println("consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey());
                        stringBuilder.append("INFO consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey()).append("\n");
                        try (JsonReader source = new JsonReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(keypair.getValue()))))) {
                            JsonObject payload = new Gson().fromJson(source, JsonObject.class);
                            JsonArray nestedJson = payload.getAsJsonArray("kustomer");
                            nestedJson.forEach(y -> {
                                if (validate(y.toString())) {
                                    /*only take payloads with a valid data key, if not payload will pass nullpointerexception*/
                                    if (y.getAsJsonObject().has("data")) {
                                        JsonObject att = y.getAsJsonObject().get("data").getAsJsonObject().get("attributes").getAsJsonObject();
                                        piiArray.forEach(anonymizedKey -> {
                                            if (att.has(anonymizedKey) && !att.get(anonymizedKey).isJsonNull()) {
                                                switch (anonymizedKey) {
                                                    case "emails":
                                                        att.getAsJsonArray(anonymizedKey).forEach(e -> e.getAsJsonObject().addProperty("email", REPLACE));
                                                        break;
                                                    case "preview":
                                                        att.addProperty("preview", textMasker(att.get("preview").toString()));
                                                        break;
                                                    default:
                                                        att.addProperty(anonymizedKey, REPLACE);
                                                }
                                            }
                                        });
                                    }
                                }
                            });
                            try (
                                    ByteArrayOutputStream compressRaw = new CompressWrite().writestreamGZIP(payload.toString());
                                    ByteArrayInputStream bios = new ByteArrayInputStream(compressRaw.toByteArray())
                            ) {
                                ObjectMetadata objectMetadata = new ObjectMetadata();
                                objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                                objectMetadata.setContentLength(compressRaw.size());
                                PutObjectRequest por = new PutObjectRequest(s3target, "anonymized/" + keypair.getKey(), bios, objectMetadata);
                                cli.putObject(por);
                            }
                        } catch (Exception e) {
                            StackTraceElement[] trace = e.getStackTrace();
                            for (StackTraceElement etrace : trace) {
                                System.out.println(etrace);
                                stringBuilder.append(etrace).append("\n");
                            }
                        }
                    });
        } catch (Exception e) {
            StackTraceElement[] trace = e.getStackTrace();
            for (StackTraceElement etrace : trace) {
                System.out.println(etrace);
                stringBuilder.append(etrace).append("\n");
            }
        }
    }

    private void events() {
        String REPLACE = "(anonymized PII)";
        try {
            lbqFileMap
                    .poll()
                    .entrySet()
                    .parallelStream()
                    .forEach(keypair -> {
                        System.out.println("consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey());
                        stringBuilder.append("INFO consumer thread: " + Thread.currentThread().getName() + "->" + keypair.getKey()).append("\n");
                        try (
                                JsonReader source = new JsonReader(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(keypair.getValue()))))
                        ) {
                            JsonObject payload = new Gson().fromJson(source, JsonObject.class);
                            JsonArray nestedJson = payload.get("qsr").getAsJsonArray();
                            nestedJson.forEach(parentArray -> {
                                if (parentArray.getAsJsonObject().has("courses")) {
                                    /* courses nested JSON where all PII lives in  QSR */
                                    parentArray.getAsJsonObject().get("courses").getAsJsonArray().forEach(
                                            x -> {
                                                JsonObject courses = x.getAsJsonObject();
                                                /*QSR PII KEYS*/
                                                piiArray.forEach(anonymizedKey -> {
                                                    if (courses.has(anonymizedKey) && anonymizedKey.equals("customername")) {
                                                        courses.addProperty(anonymizedKey, REPLACE);
                                                    } else if (courses.has(anonymizedKey) && anonymizedKey.equals("tablename")) {
                                                        courses.addProperty(anonymizedKey, REPLACE);
                                                    } else if (courses.has(anonymizedKey) && anonymizedKey.equals("server")) {
                                                        courses.get(anonymizedKey).getAsJsonObject().addProperty("name", REPLACE);
                                                    }
                                                });

                                            }
                                    );
                                }
                            });
                            try (
                                    ByteArrayOutputStream compressRaw = new CompressWrite().writestreamGZIP(payload.toString());
                                    ByteArrayInputStream bios = new ByteArrayInputStream(compressRaw.toByteArray())
                            ) {
                                ObjectMetadata objectMetadata = new ObjectMetadata();
                                objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                                objectMetadata.setContentLength(compressRaw.size());
                                PutObjectRequest por = new PutObjectRequest(s3target, "anonymized/" + keypair.getKey(), bios, objectMetadata);
                                cli.putObject(por);
                            }
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            stringBuilder.append(e.getMessage()).append("\n");
                            StackTraceElement[] trace = e.getStackTrace();
                            for (StackTraceElement etrace : trace) {
                                System.out.println(etrace);
                                stringBuilder.append(etrace).append("\n");
                            }
                        }
                    });
        } catch (Exception e) {
            System.out.println(e.getMessage());
            stringBuilder.append(e.getMessage()).append("\n");
            StackTraceElement[] trace = e.getStackTrace();
            for (StackTraceElement etrace : trace) {
                System.out.println(etrace);
                stringBuilder.append(etrace).append("\n");
            }
        }
    }

    public String textMasker(String source) {
        String email = "([^.@\\s]+)(\\.[^.@\\s]+)*@([^.@\\s]+\\.)+([^.@\\s]+)";
        String phone = "\\d{10}|(?:\\d{3}-){2}\\d{4}|\\(\\d{3}\\)\\d{3}-?\\d{4}";
        String fullName = "^[A-Za-z.\\s_-]+$";
        String REPLACE = "(Anonymized PII)";

        return source.replaceAll(email, REPLACE).replaceAll(phone, REPLACE).replaceAll(fullName, REPLACE);
    }

    private boolean validate(String payload) {
        Gson parser = new Gson();
        return parser.fromJson(payload, Object.class).getClass().equals(String.class) ? false : true;

    }

}
