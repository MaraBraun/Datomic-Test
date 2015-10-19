import datomic.Connection;
import datomic.Database;
import datomic.Peer;

import java.util.concurrent.BlockingQueue;

/**
 * Created by MBN on 29.07.2015.
 */
public class QueryThread implements Runnable {
    //private Database db;
    private int operations;

    public QueryThread(BlockingQueue<Connection> queue, int operations) {
        this.operations = operations;
        this.queue = queue;
    }

    private BlockingQueue<Connection> queue;

    public void run() {
        try {
            Connection conn = queue.take();
            Database myDb = conn.db();
            dbQuery(operations, 0, myDb);
            queue.put(conn);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    public static void dbQuery(int numberOfRequests, long time, Database database) throws InterruptedException {
        for(int i=0; i < numberOfRequests; i++){
            Thread.sleep(time);
            Peer.query("[:find ?c :where [?c :community/name]]", database);
        }

    }
}
