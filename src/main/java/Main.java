import datomic.Connection;
import datomic.Database;
import datomic.Peer;
import datomic.Util;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by Mara on 19.10.2015.
 */
public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        DatomicSandbox datomicSandbox = new DatomicSandbox();
        String uri = "datomic:dev://46.101.169.9:80/seattle";
//        init(uri);
        Connection con = Peer.connect(uri);
        datomicSandbox.insert(con);
        Database db = con.db();
        String result = datomicSandbox.query(db);
        System.out.println(result);
        con.release();
    }

    public static void init(String uri){
        Peer.createDatabase(uri);
        Connection conn = Peer.connect(uri);
        String schema = "D:\\Programme\\datomic-pro-0.9.5173\\samples\\seattle\\seattle-schema.edn";
        String data = "D:\\Programme\\datomic-pro-0.9.5173\\samples\\seattle\\seattle-data0.edn";
        try {
            fillDB(schema, conn);
            fillDB(data, conn);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        conn.release();
    }

    public static void fillDB(String data, Connection conn) throws IOException, ExecutionException, InterruptedException {
        FileReader schema_rdr = new FileReader(data);
        List<String> dbschema = (List) Util.readAll(schema_rdr).get(0);
        conn.transact(dbschema).get();
        schema_rdr.close();
    }
}
