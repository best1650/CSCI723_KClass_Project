import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class PrintAssignment7TestGraph {
	public static void main(String args[]) {
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(args[0]))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
		Transaction tx = db.beginTx();
		
		// To print all nodes properties
		
//		Result res = db.execute("MATCH (v) "
//				+ "WITH ID(v) AS id, LABELS(v) as label, v.KClass as kclass, v.KClassUpperBound as kcup, v.OriginalId as originalid "
//				+ "RETURN id, label, kclass, kcup, originalid");
//		while(res.hasNext()) {
//			Map<String,Object> resMap = res.next();
//			System.out.println(resMap);
//		}
		
		// To print nodes and their neighbors in order
		
//		for(long i = 0; i < 14; i++) {
//			Map<String,Object> par = new HashMap<>();
//			par.put("id",i);
//			res = db.execute("MATCH (v)--(n) "
//					+ "WITH ID(v) AS id, LABELS(v) as label, ID(n) AS nbrid, LABELS(n) AS nbrlabel "
//					+ "WHERE id = $id "
//					+ "RETURN id, label, nbrid, nbrlabel",par);
//			while(res.hasNext()) {
//				Map<String,Object> resMap = res.next();
//				System.out.println(resMap);
//			}
//		}
		
		// To print nodes only if their degree matches respective upper bound or kclass value
		Result res = db.execute("MATCH (v) "
				+ "WITH ID(v) AS id, LABELS(v) as label, size((v)--()) as degree, v.KClass as kclass, v.KClassUpperBound as kcup "
				+ "RETURN id, label, degree, kclass, kcup");
		while(res.hasNext()) {
			Map<String,Object> resMap = res.next();
			Long degree = (Long)resMap.get("degree");
			int kcup = (int)resMap.get("kcup");
			int kclass = (int)resMap.get("kclass");
			if(degree > 1 && degree == kcup)
				System.out.println(resMap);
			else if(degree == 1 && degree == kclass)
				System.out.println(resMap);
		}
		
		tx.close();
		db.shutdown();
	}

}
