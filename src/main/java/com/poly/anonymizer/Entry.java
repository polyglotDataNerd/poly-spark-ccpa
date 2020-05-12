package com.poly.anonymizer;

import com.poly.utils.ConfigProps;
import com.poly.utils.EmailWrapper;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;

public class Entry implements Serializable {

    private SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");

    public void run(String sourceBucket, String sourceKey, String loadFormat, String targetBucket, String piiArray, StringBuilder stringBuilder) throws Exception {
        ConfigProps config = new ConfigProps();
        Logger log = LogManager.getLogger(Entry.class);
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        ArrayList<String> pii = new ArrayList<>(Arrays.asList(piiArray.split(",")));
        /*multi-threaded anonymizer*/
        if (loadFormat.contains("")) {
            int threads = loadFormat.contains("adhoc") ? Runtime.getRuntime().availableProcessors() - 1 : 1;
            System.out.println("Number of threads: " + threads);
            stringBuilder.append("INFO Number of threads: " + threads).append("\n");
            processQueue(sourceBucket, sourceKey, loadFormat, targetBucket, pii, stringBuilder, log, threads);
            stopwatch.stop();
            System.out.println("INFO app run length (seconds): " + stopwatch.getTime(TimeUnit.SECONDS));
            stringBuilder.append("INFO app run length (seconds): " + stopwatch.getTime(TimeUnit.SECONDS)).append("\n");
        } else {
            processQueue(sourceBucket, sourceKey, loadFormat, targetBucket, pii, stringBuilder, log, Runtime.getRuntime().availableProcessors() - 1);
            stopwatch.stop();
            System.out.println("INFO app run length (seconds): " + stopwatch.getTime(TimeUnit.SECONDS));
            stringBuilder.append("INFO app run length (seconds): " + stopwatch.getTime(TimeUnit.SECONDS)).append("\n");
        }

        new EmailWrapper(
                config.getPropValues("emails"),
                config.getPropValues("fromemails"),
                "ETL Notification " + ft.format(new Date()) + " SPARK-JAVA: " + " CCPA JSON anonymizer " + loadFormat,
                stringBuilder.toString()).sendEMail();
    }

    private void processQueue(String srcBucket, String sourceKey, String loadFormat, String targetBucket, ArrayList<String> piiArray, StringBuilder stringBuilder, Logger log, int numThreads) {
        try {
            ConcurrentLinkedDeque<Map<String, byte[]>> lbqFileMap = new ConcurrentLinkedDeque<>();
            /*numThreads control threads for smaller task processing for some data sources*/
            /*https://www.baeldung.com/java-executor-wait-for-threads*/
            CountDownLatch latch = new CountDownLatch(numThreads);
            ExecutorService exec = Executors.newFixedThreadPool(numThreads);
            ObjectAnonymizer consumer = new ObjectAnonymizer(loadFormat, lbqFileMap, piiArray, stringBuilder, targetBucket);
            S3Dir producer = new S3Dir(srcBucket, sourceKey, lbqFileMap, stringBuilder);

            /*recursively scans object directory to get all key value pairs*/
            exec.submit(() -> {
                try {
                    producer.run();
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });

            /*lets the producer catch up before it starts consuming*/
            if (loadFormat.contains("source")) {
                Thread.sleep(30000);
            }
            if (loadFormat.contains("source")) {
                Thread.sleep(30000);
            }
            if (loadFormat.contains("source")) {
                Thread.sleep(60000);
            }
            if (loadFormat.contains("source")) {
                Thread.sleep(10000);
            }
            /*lets the producer catch up before it starts consuming*/

            /*process files with N Threads*/
            for (int i = 0; i < numThreads; i++) {
                exec.submit(() -> {
                    try {
                        consumer.run();
                    } catch (Exception e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                });

            }
            latch.await();
            exec.shutdown();
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(e.getCause());
            StackTraceElement[] trace = e.getStackTrace();
            for (StackTraceElement etrace : trace) {
                log.error(etrace);
            }
        }
    }
}
