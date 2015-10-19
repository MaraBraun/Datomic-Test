import datomic.Connection;
import datomic.Peer;
import datomic.Util;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

/**
 * Created by MBN on 29.07.2015.
 */
public class InsertThread implements Runnable{
    private BlockingQueue<Connection> queue;
    private int operations;

    public InsertThread(BlockingQueue<Connection> queue, int operations) {
        this.queue = queue;
        this.operations = operations;
    }

    public void run() {
        Connection con = null;
        try {
            con = queue.take();
            dbInsert(operations, 0, con);
            queue.put(con);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    public static void dbInsert(int numberOfRequests, long time, Connection connection) throws InterruptedException {
        Random random = new Random(System.currentTimeMillis());
        for(int i=0; i < numberOfRequests; i++){
            Thread.sleep(time);
            Object temp_id = Peer.tempid(":db.part/user");
            List tx = Util.list(Util.map(
                    ":district/region", ":region/e",
                    ":db/id", temp_id,
                    ":district/name", ((Double)random.nextDouble()).toString()));
            connection.transact(tx);
        }

    }
}
