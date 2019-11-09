import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.graphdb.RelationshipType;

public class EMCore {

	// Public members
	public static GraphDatabaseService graphDB;
	public static Transaction tx ;
	public static String auxDBPath;
	public static String srcDBPath;
	
	public static void createDB(String neo4jFolder)
	{
//		GraphDatabaseFactory gdbf = new GraphDatabaseFactory();
//		graphDB = gdbf.newEmbeddedDatabase(new File(neo4jFolder));
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
		tx = graphDB.beginTx();
		System.out.println("DB service created");
		/*
		try 
		{
            for (String fileName : listOfTargets.split(","))
            {
				 Transaction tx = db.beginTx();
				 db.execute("CREATE INDEX ON :`" + fileName + "`(origID)");
				 tx.success();
				 tx.close();
            }
		} 
		catch (Exception oops) 
		{
			oops.printStackTrace();
		}
		*/
	}
	
	public static void closeDB()
	{
		tx.close();
		graphDB.shutdown();
	}
	
	public static Set<Long> getListSortedByVal(HashMap<Long, Long> hm)
	{
		 List<Map.Entry<Long, Long> > list = new LinkedList<Map.Entry<Long, Long> >(hm.entrySet()); 
		 Collections.sort(list, new Comparator<Map.Entry<Long, Long> >() 
		 { 
			 @Override
			 public int compare(Entry<Long, Long> arg0, Entry<Long, Long> arg1)
			 {
				 // TODO Auto-generated method stub
				 return (arg0.getValue()).compareTo(arg1.getValue()); 
			 } 
		 }); 
		 
		 Set<Long> rtnSet = new HashSet<Long>();
		 
		 for (Map.Entry<Long, Long> item : list) 
		 { 
			 rtnSet.add(item.getKey()); 
	     } 
		 
		 return rtnSet;
	}
	

	/**********************************************************************
	 * 
	 * Redefine the KClass upper bound for the input graph
	 * @return None
	 * 
	 **********************************************************************/
	public static void refinedUpperBound(Set<Long> inputSet, int limit)
	{
		// Iterator counter
		int iter = 0;
		
		HashMap<Long, Long> kcupMap = new HashMap<Long, Long>();
		Map<String,Object> parameter = new HashMap<>();
				
		// Stop the process the number of iteration hits the given limit
		// TODO iter <= limit
		while (iter < limit && inputSet.size() > 0)
		{
			// Clear the map
			kcupMap.clear();
			
			// foreach v in the input list
			int count = 0;
			for(Long v : inputSet)
			{
				// Get KClass Upper Bound from vertex map
				// See description for class variables
//				parameter.put("id",v);
				Map<String,Object> v_prop = getNodeProperties(v);
				kcupMap.put(v, (long)v_prop.get("k"));
				count++;
				if(count%1000 == 0)
					System.out.println(count);
			}
			
			inputSet = getListSortedByVal(kcupMap);
			System.out.println("inputSet sorted "+inputSet.size());
			
			// Create an empty list
			Set<Long> nextList = UpperBoundReduction(inputSet);
			
			// For next iteration
			inputSet = new HashSet<Long>(nextList);
			iter++;
		}
	}
	
	// Implement by Pansa
	public static Set<Long> UpperBoundReduction(Set<Long> inputSet)
	{
		Set<Long> rtnSet = new HashSet<>();
		int count = 0;
		for(Long v : inputSet) {
			ArrayList<Long> Z = new ArrayList<>();
			Map<Long,Long> Z_Map = new HashMap<>();
			Map<String,Object> par = new HashMap<>();
			Map<String,Object> v_prop = getNodeProperties(v);
			par.put("id",v);
			long v_degree = (Long)v_prop.get("d");
			long v_kcub = (long)v_prop.get("k");
			par.put("d", v_kcub);
			
			// Get degree of neighbors of v
			Result res = graphDB.execute("MATCH (v)--(v1) "
					+ "WITH ID(v) AS id, coalesce(v1.KClass,v1.KClassUpperBound) as k, ID(v1) as nbid "
	    			+ "WHERE id = $id AND EXISTS(v.KClassUpperBound) AND k < $d "
	    			+ "RETURN nbid, k "
	    			+ "ORDER BY k ASC",par);
			while(res.hasNext()) {
				Map<String,Object> resMap = res.next();
				Long zid =(Long)resMap.get("nbid");
				Long zk = (Long)resMap.get("k");
				Z.add(zid);
				Z_Map.put(zid,zk);
			}
			if((v_degree - Z.size()) < v_kcub) {
				long min = Long.MAX_VALUE;
				boolean flag = false;
				for(int i = 1; i <= Z.size(); i++) {
//					Map<String,Object> u_prop = getNodeProperties(Z.get(i-1));
//					long u_kcub = (long)u_prop.get("k");
					long c = Math.max(v_degree-i, Z_Map.get(Z.get(i-1)));
					if(c < min) {
						min = c;
						flag = true;
					}
				}
				if(flag) {
					par.put("min",min);
					graphDB.execute("MATCH (v) "
							+ "WHERE ID(v)=$id "
							+ "SET v.KClassUpperBound = $min ",par);
				}
				for(int i = 0; i < Z.size(); i++)
					rtnSet.add(Z.get(i));
			}
			count++;
			if(count%1000 == 0)
				System.out.println(count + " ubr");
		}
		System.out.println("Returning from ubr " + rtnSet.size());
		return rtnSet;
	}
	
	// Main Function
	public static void main(String[] args) 
	{
		// Input arguments from the main function
//		srcDBPath = args[0];
//		auxDBPath = args[1];
		srcDBPath = "/home/sapan/Documents/CSCI-723-GraphDB/Assignment7/EnronDB";
		auxDBPath= "/home/sapan/Documents/CSCI-723-GraphDB/AuxDB";
		int KClass = Integer.parseInt(args[2]);
		int limit = Integer.parseInt(args[3]);
		float p = Float.parseFloat(args[4]);
		int stepover = Integer.parseInt(args[5]);
		
		// Create the graph database with the given source Neo4j path
		createDB(srcDBPath);
		KCoreDecomposition(KClass, limit, stepover);
		printTestGraph();

		closeDB();

		System.out.println("Job Completed!");
		closeDB();

	}
	
	// To be implemented by Sapan
	public static void KCoreDecomposition(int mink, int limit, int step) {
		System.out.println("KCoreDecomposition");
		Result res = graphDB.execute("MATCH (v) "
				+ "WITH size((v)--()) as degree, ID(v) as id "
				+ "RETURN degree,id ");
		System.out.println("Got node id and degree");
		Set<Long> W = new HashSet<>();		
		while(res.hasNext()) {
			// reference to query result
			Map<String,Object> resMap = res.next();
			Map<String,Object> parameter = new HashMap<>();
			long degree = (Long)resMap.get("degree");
			long id = (Long)resMap.get("id");
			long p = 0;
			parameter.put("id",id);
			parameter.put("degree",degree);
			parameter.put("p",p);
			if(degree == 1) {
				graphDB.execute("MATCH (v) "
						+ "WHERE ID(v)=$id "
						+ "SET v.KClass = $degree, v.KClassUpperBound = null, v.deposit = $p, v.degree = $degree",parameter);
				tx.success();
			}
			else {
				graphDB.execute("MATCH (v) "
						+ "WHERE ID(v)=$id "
						+ "SET v.KClass = null, v.KClassUpperBound = $degree, v.deposit = $p, v.degree = $degree ",parameter);
				tx.success();
				W.add(id);
			}
		}
		System.out.println("Calling Refine Upper Bound 1 " + W.size());

		refinedUpperBound(W,limit);
		System.out.println("Returned");

		long Ku = 0;
		do {
			Ku = calculateKUpper();
			long Kl = Ku - step;
			W = new HashSet<>();
			Map<String,Object> parameter = new HashMap<>();
			parameter.put("Kl",Kl);
			parameter.put("Ku",Ku);
			res = graphDB.execute("MATCH (v) "
					+ "WITH coalesce(v.KClass, v.KClassUpperBound) as k, ID(v) as id "
					+ "WHERE k >= $Kl AND k <= $Ku "
					+ "RETURN id",parameter);
			while(res.hasNext()) {
				W.add((long)res.next().get("id"));
			}
			Set<Long> M = new HashSet<>();
			Map<Long,Long> Kc = new HashMap<>();
			if(W.size() >= Kl) {
				// TODO 
				// Create graph and call computecore function
				createAuxGraphDB(W);
				GraphDatabaseService auxDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(auxDBPath))
						.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
						.setConfig(GraphDatabaseSettings.string_block_size, "60" )
						.setConfig(GraphDatabaseSettings.array_block_size, "300" )
						.newGraphDatabase();
				Transaction auxTx = auxDB.beginTx();
				Kc = ComputeCore(auxDB, Kl, Ku);
				auxTx.close();
				auxDB.shutdown();
				
			}
			for(long v: W) {
				parameter.put("id",v);
				parameter.put("kcl",Kc.get(v));
				parameter.put("kcub",Kl-1);
				if(Kc.containsKey(v)) {
					graphDB.execute("MATCH (v) "
							+ "WHERE ID(v)=$id "
							+ "SET v.KClass = $kcl, v.KClassUpperBound = null ",parameter);
					tx.success();
				}
				else {
					graphDB.execute("MATCH (v) "
							+ "WHERE ID(v)=$id "
							+ "SET v.KClassUpperBound = $kcub ",parameter);
					tx.success();
					M.add(v);
					
				}
			}
			parameter.clear();
			for(long v: Kc.keySet()) {
				parameter.put("id",v);
				res = graphDB.execute("MATCH ((v)--(v1)) "
						+ "WHERE ID(v) = $id "
						+ "RETURN ID(v1) AS nbid",parameter);
				while(res.hasNext()) {
					long n = (long)res.next().get("nbid");
					Map<String,Object> n_prop = getNodeProperties(n);
					parameter.put("id", n);
					parameter.put("p", (long)n_prop.get("p")+1);
					graphDB.execute("MATCH (v) "
							+ "WHERE ID(v) = $id "
							+ "SET v.deposit = $p ",parameter);
					tx.success();
					if(n_prop.get("k") != null) {
						M.add(n);
					}
				}
			}
			System.out.println("Refine Upper Bound 2");
			refinedUpperBound(M,limit);

		}
		while(Ku > mink);
		
	}
	
	public static void createAuxGraphDB(Set<Long> W)
	{
		HashMap<String, Object> parameter = new HashMap<String, Object>();
		String nodeQuery = "MATCH (n) WHERE ID(n) = $id RETURN n.deposit as deposit";
		String neighborQuery = "MATCH (n1)--(n2) WHERE ID(n1) = $id RETURN ID(n2) AS id ORDER BY id";
		BatchInserter inserter = null;
		
		try 
		{
			inserter = BatchInserters.inserter(new File(auxDBPath));
			for (Long w : W)
			{
				if(!inserter.nodeExists(w))
					inserter.createNode(w, parameter);
			}
			
			ArrayList<Long> neighborList = new ArrayList<Long>();
			for (Long w : W)
			{
				parameter.clear();
				neighborList.clear();
				parameter.put("id",w);
				Result res = graphDB.execute(neighborQuery, parameter);
				while(res.hasNext()) 
				{
					neighborList.add((long)res.next().get("id"));
				}
				neighborList.retainAll(W);
				
				res = graphDB.execute(nodeQuery, parameter);
				parameter.clear();
				while(res.hasNext()) 
				{
					parameter.put("deposit", (long)res.next().get("deposit"));
				}
				
				parameter.put("degree", (long)neighborList.size());
				inserter.setNodeProperties(w, parameter);
				
				for (Long nw : neighborList)
				{
					if (w > nw)
					{
						inserter.createRelationship(w, nw, RelationshipType.withName(""), null);
					}
				}
			}

			inserter.shutdown();
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			inserter.shutdown();
			e.printStackTrace();
		}
	}
	
	// To be implemented by Li
	public static Map<Long, Long> ComputeCore(GraphDatabaseService auxdDB, long kl, long ku)
	{
		Map<Long, Long> kcMap = new HashMap<Long, Long>();
		String query = "MATCH (v) RETURN ID(v) AS id, v.deposit AS deposit, v.degree AS degree";
		String delNode = "MATCH (n) where ID(n)=$id OPTIONAL MATCH (n)-[r]-() DELETE r, n";
		Map<String, Object> parameter = new HashMap<String, Object>();
		
		for (long i = kl; i <= ku; i++)
		{
			boolean isDelete = true;
			
			while(isDelete)
			{
				isDelete = false;
				
				Result res = auxdDB.execute(query);
				while(res.hasNext()) 
				{
					Map<String,Object> resMap = res.next();
					long degree = (long)resMap.get("degree");
					long deposit = (long)resMap.get("deposit");
					Long vid = (Long)resMap.get("id");	
					
					if ( (degree + deposit) < i )
					{
						isDelete = true;
						// remove nodes and edges
						parameter.clear();
						parameter.put("id", vid);
						auxdDB.execute(delNode, parameter);
					}
				}
			}
			
			Result res = auxdDB.execute(query);
			while(res.hasNext()) 
			{
				Map<String,Object> resMap = res.next();
				Long vid = (Long)resMap.get("id");
				kcMap.put(vid, i);
			}
		}
		System.out.println("AuxDBCreated");
		return kcMap;
	}
	
	public static Map<String,Object> getNodeProperties(Long id) {
		Map<String,Object> par = new HashMap<>();
		par.put("id",id);
		Result res = graphDB.execute("MATCH (v) "
				+ "WITH ID(v) as id, v.degree as d, coalesce(v.KClassUpperBound,v.KClass) as k, v.deposit as p "
				+ "WHERE id = $id "
				+ "RETURN k,p,d", par);
		Map<String,Object> resMap = new HashMap<>(res.next());
//		System.out.println("Return node properties " + id);
		return resMap;
	}
	
	public static void printTestGraph() {
		Result res = graphDB.execute("MATCH (v) "
				+ "WITH ID(v) AS id, LABELS(v) as label, size((v)--()) as degree, coalesce(v.KClass,v.KClassUpperBound) as k, v.deposit as p "
				+ "RETURN id, label, degree, k, p");
		while(res.hasNext()) {
			Map<String,Object> resMap = res.next();
			System.out.println(resMap);
		}	
	}
	
	public static long calculateKUpper() {
		Result res = graphDB.execute("MATCH (v) "
				+ "WHERE EXISTS(v.KClassUpperBound) "
				+ "WITH v.KClassUpperBound as k "
				+ "RETURN k");
		long max = Long.MIN_VALUE;
		while(res.hasNext()) {
			long k = (Long)res.next().get("k");
			if(max < k)
				max = k;
		}
		System.out.println("KUpper calculated");
		return max;
	}

}
