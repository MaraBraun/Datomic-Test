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
public class DatomicSandbox {

    public void insert(Connection connection){
        Object temp_id = Peer.tempid(":db.part/user");
        List tx = Util.list(Util.map(
                ":district/region", ":region/e",
                ":db/id", temp_id,
                ":district/name", "Hello World!"));
        connection.transact(tx);
    }

    public String query(Database database){
        Object result = Peer.query("[:find ?c :where [?c :community/name]]", database);
        return result.toString();
    }
}
