/**
 * Created by MBN on 20.07.2015.
 */

import datomic.Connection;
import datomic.Database;
import datomic.Peer;
import datomic.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class DatomicTester {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
//        String uri = "datomic:couchbase://192.168.56.101/datomic/seattle?password=datomic";
//        Peer.deleteDatabase(uri);
//        createDB();
        List<String> csvInsertStrings = new ArrayList<String>(6);
        List<String> csvQueryStrings = new ArrayList<String>(6);
        csvInsertStrings.add(generateCSVString("Threads", "Number of Operations", "average Time"));


        csvInsertStrings.add(setupAndRun(10, 256, 10000, 'I'));
        csvInsertStrings.add(setupAndRun(10, 512, 10000, 'I'));
        csvInsertStrings.add(setupAndRun(10, 1024, 10000, 'I'));
//
////        csvInsertStrings.add(setupAndRun(10, 2, 10000, 'I'));
////        csvInsertStrings.add(setupAndRun(10, 4, 10000, 'I'));
////        csvInsertStrings.add(setupAndRun(10, 8, 10000, 'I'));
////        csvInsertStrings.add(setupAndRun(10, 16, 10000, 'I'));
////        csvInsertStrings.add(setupAndRun(10, 32, 10000, 'I'));
//
        csvQueryStrings.add(generateCSVString("Threads", "Number of Operations", "average Time"));


        csvQueryStrings.add(setupAndRun(10, 256, 10000, 'Q'));
        csvQueryStrings.add(setupAndRun(10, 512, 10000, 'Q'));
        csvQueryStrings.add(setupAndRun(10, 1024, 10000, 'Q'));
//
////        csvQueryStrings.add(setupAndRun(10, 2, 10000, 'Q'));
////        csvQueryStrings.add(setupAndRun(10, 4, 10000, 'Q'));
////        csvQueryStrings.add(setupAndRun(10, 8, 10000, 'Q'));
////        csvQueryStrings.add(setupAndRun(10, 16, 10000, 'Q'));
////        csvQueryStrings.add(setupAndRun(10, 32, 10000, 'Q'));
////
        generateCSV("D:\\docs\\insert-thread.csv", csvInsertStrings);
        generateCSV("D:\\docs\\query-thread.csv", csvQueryStrings);
       // Peer.shutdown(true);
        System.exit(0);
    }

    public static long executeInsertTest(int ops, int workerCount, BlockingQueue<Connection> connections, ExecutorService executor) throws InterruptedException, ExecutionException {
        List<Future> results = new ArrayList<Future>(workerCount);
        long timeBefore = System.currentTimeMillis();
        for (int i = 0; i < workerCount; i++) {
            Runnable inserter = new InsertThread(connections, ops / workerCount);
            results.add(executor.submit(inserter));
            //executor.execute(inserter);
        }
        for (Future f : results) {
            System.out.println("LOL");
            f.get();
        }
        long timeAfter = System.currentTimeMillis();

        return (timeAfter - timeBefore);

    }

    public static String setupAndRun(int numOfRuns, int threadCounter, int numberOfOperations, char testType) throws ExecutionException, InterruptedException {
        List<Long> durations = new ArrayList<Long>(numOfRuns);
        String uri = "datomic:couchbase://192.168.56.101/datomic/seattle?password=datomic";
        ExecutorService executor = Executors.newFixedThreadPool(threadCounter);
        BlockingQueue<Connection> conQueue = new ArrayBlockingQueue<Connection>(threadCounter);
        for (int i = 0; i < threadCounter; i++) {
            conQueue.add(Peer.connect(uri));
        }

        for (int i = 0; i < numOfRuns; i++) {
            if (testType=='I'){
                durations.add(executeInsertTest(numberOfOperations, threadCounter, conQueue, executor));
            } else if(testType == 'Q'){
                durations.add(executeQueryTest(numberOfOperations, threadCounter, conQueue, executor));
            }else {
                System.out.println("Wrong type char");
                break;
            }

        }
        long before = System.currentTimeMillis();
        for(Connection con : conQueue){
            con.release();
        }
        long after = System.currentTimeMillis();
        System.out.println("Zeit für Freigabe der Verbindungen: "+(after-before)/1000+"s");

        executor.shutdown();
        long sum = 0;
        for (Long l : durations) {
            sum += l;
        }
        float avg = (float) sum / numOfRuns;
        return generateCSVString(((Integer)threadCounter).toString(),((Integer)numberOfOperations).toString(), ((Float)avg).toString());
    }

    public static long executeQueryTest(int ops, int workerCount, BlockingQueue<Connection> connections, ExecutorService executor) throws InterruptedException, ExecutionException {
        List<Future> results = new ArrayList<Future>(workerCount);
        long timeBefore = System.currentTimeMillis();
        for (int i = 0; i < workerCount; i++) {
            Runnable querier = new QueryThread(connections, ops / workerCount);
            results.add(executor.submit(querier));
        }
        for (Future f : results) {
            System.out.println("ROFL");
            f.get();
        }
        long timeAfter = System.currentTimeMillis();
        return (timeAfter - timeBefore);
    }


    public static void fillDB(String data, Connection conn) throws IOException, ExecutionException, InterruptedException {
        FileReader schema_rdr = new FileReader(data);
        List<String> dbschema = (List) Util.readAll(schema_rdr).get(0);
        conn.transact(dbschema).get();
        schema_rdr.close();
    }

    public static String generateCSVString(String x, String y, String s){
        return (x + ";" + y + ";" + s + "\n");
    }

    public static void generateCSV(String filename, List<String> csvStrings) {
        try {
            FileWriter writer = new FileWriter(filename, true);
            for (String s: csvStrings){
                writer.append(s);
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createDB() throws InterruptedException, ExecutionException, IOException {
        String uri = "datomic:couchbase://192.168.56.101/datomic/seattle?password=datomic";
         Peer.createDatabase(uri);
        Connection conn = Peer.connect(uri);
        String schema = "D:\\Program Files\\datomic-pro-0.9.5173\\datomic-pro-0.9.5173\\samples\\seattle\\seattle-schema.edn";
        String data = "D:\\Program Files\\datomic-pro-0.9.5173\\datomic-pro-0.9.5173\\samples\\seattle\\seattle-data0.edn";
        fillDB(schema, conn);
        fillDB(data, conn);
        conn.release();
    }

}