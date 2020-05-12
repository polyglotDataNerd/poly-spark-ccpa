package com.poly.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class DDB implements Serializable {
    Logger log = LogManager.getLogger(DDB.class);
    StringBuilder stringBuilder;
    private AmazonDynamoDB client = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(Regions.US_WEST_2)
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .build();

    public DDB(StringBuilder stringBuilder) {
        this.stringBuilder = stringBuilder;
    }

    /*empty constructor for non serializable objects*/
    public DDB() {
    }

    public TreeMap<String, String> getUUID(String sourceID) throws IOException {
        SGIDService ddbmappper = new SGIDService();
        TreeMap<String, String> user = new TreeMap<String, String>();
        try {
            DynamoDBMapper mapper = new DynamoDBMapper(client);
            ddbmappper.set_sysid(sourceID);
            DynamoDBQueryExpression<SGIDService> queryExpression = new DynamoDBQueryExpression<SGIDService>().withHashKeyValues(ddbmappper);
            List<SGIDService> itemList = mapper.query(SGIDService.class, queryExpression);
            IntStream.range(0, itemList.size()).forEach(i -> {
                        user.put(itemList.get(i).get_sysid(), itemList.get(i).get_uuid());
                    }
            );
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(e.getCause());
            StackTraceElement[] trace = e.getStackTrace();
            for (StackTraceElement etrace : trace) {
                log.error(etrace);
            }
        }
        return user;
    }


    public void writeItem(String sourceSystemId, String uuid) {
        try {
            DynamoDBMapper mapper = new DynamoDBMapper(client);
            SGIDService itemList = new SGIDService();
            itemList.set_sysid(sourceSystemId);
            itemList.set_uuid(uuid);
            mapper.save(itemList);
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(e.getCause());
            StackTraceElement[] trace = e.getStackTrace();
            for (StackTraceElement etrace : trace) {
                log.error(etrace);
            }
        }
    }

    public ConcurrentHashMap<String, String> parallelScan(String tableName) {
        DynamoDB dynamoDB = new DynamoDB(client);
        ConcurrentHashMap<String, String> mapper = new ConcurrentHashMap<>();
        LinkedList<ItemCollection<ScanOutcome>> linkedList = new LinkedList<>();
        StopWatch sw = new StopWatch();
        sw.start();
        Table table = dynamoDB.getTable(tableName);
        try {
            /*1000000 bytes = 1MB max payload size will derive max limit and total segments*/
            int numParallelThreads = (int) Math.ceil(client.describeTable(table.getTableName()).getTable().getTableSizeBytes() / 1000000) + 1;
            int tableCount = client.describeTable(table.getTableName()).getTable().getItemCount().intValue();
            //int itemLimit = (int) Math.ceil(tableCount / numParallelThreads);
            ExecutorService executor = Executors.newFixedThreadPool(numParallelThreads);
            System.out.println("DDB Table Count = " + String.format("%,d", tableCount));
            stringBuilder.append("DDB Table Count = " + String.format("%,d", tableCount)).append("\n");
            int segment = 0;
            for (; segment < numParallelThreads; segment++) {
                DDBScanner scanner = new DDBScanner(table, numParallelThreads, segment, linkedList);
                executor.execute(scanner);
            }
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
            linkedList.parallelStream().forEach(items -> {
                        items.forEach(kv -> {
                            try {
                                mapper.put(kv.get("sourceSystemId").toString(), kv.get("uuid").toString());
                            } catch (Exception e) {
                                StackTraceElement[] trace = e.getStackTrace();
                                for (StackTraceElement etrace : trace) {
                                    log.error(etrace);
                                }
                            }
                        });
                    }
            );
            /*gets the last batch of the scanned items of the table using the last evaluated key
            Map<String, AttributeValue> lastKeyEvaluated = linkedList.peekLast().getLastLowLevelResult().getScanResult().getLastEvaluatedKey();
            System.out.println(lastKeyEvaluated.toString());
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tableName)
                    //.withLimit(tableCount)
                    .withExclusiveStartKey(lastKeyEvaluated);
            client.scan(scanRequest).getItems().parallelStream().forEach(items -> {
                mapper.put(items.get("sourceSystemId").getS(), items.get("uuid").getS());
            });*/

        } catch (Exception e) {
            log.error(e.getMessage());
            log.error(e.getCause());
            StackTraceElement[] trace = e.getStackTrace();
            for (StackTraceElement etrace : trace) {
                log.error(etrace);
            }
        }
        sw.stop();
        stringBuilder.append("Parallel Scan End in seconds " + sw.getTime(TimeUnit.SECONDS)).append("\n");
        System.out.println("Parallel Scan End in seconds " + sw.getTime(TimeUnit.SECONDS));
        return mapper;
    }

    public long getDDBTableCount(String tableName) {
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        return client.describeTable(table.getTableName()).getTable().getItemCount();
    }

    private class DDBScanner implements Runnable {
        Logger log = LogManager.getLogger(DDBScanner.class);
        // DDB ID service
        private Table table;
        // number of items each scan request should return
        // Equals to total number of threads scanning the table in parallel
        private int totalSegments;
        // Segment that will be scanned with by this task
        private int segment;
        // linkedList to get Scanned DDB items
        private LinkedList<ItemCollection<ScanOutcome>> linkedList;

        public DDBScanner(Table table, int totalSegments, int segment, LinkedList<ItemCollection<ScanOutcome>> linkedList) {
            this.table = table;
            this.totalSegments = totalSegments;
            this.segment = segment;
            this.linkedList = linkedList;
        }


        @Override
        public void run() {
            System.out.println("Scanning " + table.getTableName() + " segment " + segment + " out of " + totalSegments
                    + " segments");
            try {
                ScanSpec spec = new ScanSpec().withTotalSegments(totalSegments).withSegment(segment);
                ItemCollection<ScanOutcome> items = table.scan(spec);
                linkedList.add(items);
            } catch (Exception e) {
                log.error(e);
                StackTraceElement[] trace = e.getStackTrace();
                for (StackTraceElement etrace : trace) {
                    log.error(etrace);
                }
            }
        }
    }

}

