import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.factory.Sets;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class EMCore {

	// Public members
	public static GraphDatabaseService graphDB;
	public static Transaction tx ;
	
	// vertex to list of properties 
	// Property list description below 
	// index : property name
	// 0 : deposit
	// 1 : KClass
	// 2 : KClassUpperBound
	// 3 : degree
	public static Map<Long,List<Integer>> vertexMap;
	
	// vertex to original id
	public static Map<Long,Long> originalIdMap;
	
	public static void createDB(String neo4jFolder)
	{
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
		tx = graphDB.beginTx();
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
	
	public static ArrayList<Long> getListSortedByVal(HashMap<Long, Long> hm)
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
		 
		 ArrayList<Long> rtnList = new ArrayList<Long>();
		 
		 for (Map.Entry<Long, Long> item : list) 
		 { 
			 rtnList.add(item.getKey()); 
	     } 
		 
		 return rtnList;
	}
	

	/**********************************************************************
	 * 
	 * Redefine the KClass upper bound for the input graph
	 * @return None
	 * 
	 **********************************************************************/
	public static void refinedUpperBound(ArrayList<Long> inputList, int limit)
	{
		// Iterator counter
		int iter = 0;
		
		HashMap<Long, Long> kcupMap = new HashMap<Long, Long>();
		Map<String,Object> parameter = new HashMap<>();
				
		// Stop the process the number of iteration hits the given limit
		while (iter < limit && inputList.size() > 0)
		{
			// Clear the map
			kcupMap.clear();
			
			// foreach v in the input list
			for(Long v : inputList)
			{
				// Get KClass Upper Bound from vertex map
				// See description for class variables
				parameter.put("id",v);
				Map<String,Object> v_prop = getNodeProperties(v);
				kcupMap.put(v, (long)v_prop.get("k"));
			}
			
			inputList = getListSortedByVal(kcupMap);
			System.out.println(inputList);
			
			// Create an empty list
			ArrayList<Long> nextList = UpperBoundReduction(inputList);
			
			// For next iteration
			inputList = new ArrayList<Long>(nextList);
			iter++;
		}
	}
	
	// Implement by Pansa
	public static ArrayList<Long> UpperBoundReduction(ArrayList<Long> inputList)
	{
		ArrayList<Long> rtnList = new ArrayList<>();
		for(Long v : inputList) {
			ArrayList<Long> Z = new ArrayList<>();
			Map<String,Object> par = new HashMap<>();
			Map<String,Object> v_prop = getNodeProperties(v);
			par.put("id",v);
			long v_degree = (Long)v_prop.get("d");
			long v_kcub = (long)v_prop.get("k");
			par.put("d", v_kcub);
			
			// Get degree of neighbors of v
			Result res = graphDB.execute("MATCH (v)--(v1) "
					+ "WITH ID(v) AS id, coalesce(v1.KClass,v1.KClassUpperBound) as k, ID(v1) as nbid "
	    			+ "WHERE id = $id AND k < $d "
	    			+ "RETURN nbid, k "
	    			+ "ORDER BY k ASC",par);
			while(res.hasNext()) {
				Map<String,Object> resMap = res.next();
				Z.add((Long)resMap.get("nbid"));
			}
			if((v_degree - Z.size()) < v_kcub) {
				long min = Long.MAX_VALUE;
				for(int i = 1; i <= Z.size(); i++) {
					Map<String,Object> u_prop = getNodeProperties(Z.get(i-1));
					long u_kcub = (long)u_prop.get("k");
					long c = Math.max(v_degree-i, u_kcub);
					if(c < min)
						min = c;
				}
				par.put("min",min);
				res = graphDB.execute("MATCH (v) "
						+ "WHERE ID(v)=$id "
						+ "SET v.KClassUpperBound = $min "
						+ "RETURN ID(v), v.KClassUpperBound",par);
				System.out.println(res.next());
				for(int i = 0; i < Z.size(); i++)
					rtnList.add(Z.get(i));
			}
		}
		
		return rtnList;
	}
	
	// Main Function
	public static void main(String[] args) 
	{
		// Input arguments from the main function
//		String srcNeo4jFolder = args[0];
//		String auxNeo4jFolder= args[1];
//		int KClass = Integer.parseInt(args[2]);
//		int limit = Integer.parseInt(args[3]);
//		float p = Float.parseFloat(args[4]);
//		int stepover = Integer.parseInt(args[5]);
//		
//		// Create the graph database with the given source Neo4j path
//		createDB(srcNeo4jFolder);
//		// K-Class upper bound estimation
//		// computeKClassUpperBound(KClass, limit);
//		// Terminate the Graph database service
//		closeDB();
		// Test Block
		createDB("/home/sapan/Documents/CSCI-723-GraphDB/Assignment7/TryGraphs");
		KCoreDecomposition(2, 1, 1);
		System.out.println(vertexMap);
		System.out.println("Job Completed!");
		closeDB();

		
	}
	
	// To be implemented by Sapan
	public static void KCoreDecomposition(int mink, int limit, int step) {
		Result res = graphDB.execute("MATCH (v) "
				+ "WITH size((v)--()) as degree, ID(v) as id "
				+ "RETURN degree,id ");
		ArrayList<Long> W = new ArrayList<>();		
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
						+ "SET v.KClass = $degree, v.KClassUpperBound = null, v.deposit = $p "
						+ "RETURN v",parameter);
			}
			else {
				graphDB.execute("MATCH (v) "
						+ "WHERE ID(v)=$id "
						+ "SET v.KClass = null, v.KClassUpperBound = $degree, v.deposit = $p "
						+ "RETURN v",parameter);
				W.add(id);
			}
		}
		tx.success();
//		System.out.println(W);
//		printTestGraph();
		refinedUpperBound(W,limit);
//		printTestGraph();
		long Ku = 0;
		do {
			Ku = calculateKUpper();
			long Kl = Ku - step;
			W = new ArrayList<>();
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
			ArrayList<Long> M = new ArrayList<>();
			Map<Long,Long> Kc = new HashMap<>();
			if(W.size() >= Kl) {
				// TODO 
				// Create graph and call computecore function
				createAuxGraphDB(W);
				String auxDBPath = "";
				GraphDatabaseService auxDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(auxDBPath))
						.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
						.setConfig(GraphDatabaseSettings.string_block_size, "60" )
						.setConfig(GraphDatabaseSettings.array_block_size, "300" )
						.newGraphDatabase();
				Transaction auxTx = auxDB.beginTx();
				
				Kc = ComputeCore(auxDB, Kl, Ku);
				
				auxDB.execute("MATCH (n) DETACH DELETE n");
				
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
							+ "SET v.KClass = $kcl, v.KClassUpperBound = null "
							+ "RETURN v",parameter);
				}
				else {
					graphDB.execute("MATCH (v) "
							+ "WHERE ID(v)=$id "
							+ "SET v.KClassUpperBound = $kcub "
							+ "RETURN v",parameter);
					M.add(v);
				}
			}
			parameter.clear();
			for(long v: Kc.keySet()) {
				parameter.put("id",v);
				res = graphDB.execute("MATCH ((v)--(v1)) "
						+ "WHERE ID(v) = $id "
						+ "RETURN ID(v1) AS nbid");
				while(res.hasNext()) {
					long n = (long)res.next().get("nbid");
					Map<String,Object> n_prop = getNodeProperties(n);
					parameter.put("id", n);
					parameter.put("p", (long)n_prop.get("p")+1);
					graphDB.execute("MATCH (v) "
							+ "WHERE ID(v) = $id "
							+ "SET v.deposit = $p "
							+ "RETURN v",parameter);
					if(n_prop.get("k") != null) {
						M.add(n);
					}
				}
			}
			refinedUpperBound(M,limit);
		}
		while(Ku > mink);
	}
	
	public static void createAuxGraphDB(ArrayList<Long> W)
	{
		String auxDBPath = "";
		HashMap<String, Object> parameter = new HashMap<String, Object>();
		String nodeQuery = "MATCH (n) WHERE ID(v) = $id RETURN v.deposit as deposit";
		String neighborQuery = "MATCH (n1)--(n2) WHERE ID(n1) = $id RETURN ID(n2) AS id ORDER BY id";
		
		try 
		{
			BatchInserter inserter = BatchInserters.inserter(new File(auxDBPath));
			
			for (Long w : W)
			{
				parameter.put("id",w);
				Result res = graphDB.execute(nodeQuery, parameter);
				parameter.clear();
				while(res.hasNext()) 
				{
					parameter.put("deposit", (long)res.next().get("deposit"));
				}
				
				inserter.createNode(w, parameter);
			}
			
			ArrayList<Long> neighborList = new ArrayList<Long>();
			
			for (Long w : W)
			{
				parameter.put("id",w);
				Result res = graphDB.execute(neighborQuery, parameter);
				while(res.hasNext()) 
				{
					neighborList.add((long)res.next().get("id"));
				}
				
				neighborList.retainAll(W);
				parameter.clear();
				parameter.put("degree", parameter.size());
				inserter.setNodeProperties(w, parameter);
				
				for (Long nw : neighborList)
				{
					if (w > nw)
					{
						inserter.createRelationship(w, nw, null, null);
					}
				}
			}
			
			inserter.shutdown();
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// To be implemented by Li
	public static Map<Long, Long> ComputeCore(GraphDatabaseService auxdDB, long kl, long ku)
	{
		Map<Long, Long> kcMap = new HashMap<Long, Long>();
		String query = "MATCH (v) RETURN ID(v) AS id, v.deposit AS deposit, v.degree AS degree";
		String delRelation = "MATCH (n)-[r]-() WITH ID(v) AS id WHERE id = $id DELETE r";
		String delNode = "MATCH (n) WITH ID(v) AS id WHERE id = $id DELETE n";
		
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
					int degree = (int)resMap.get("degree");
					int deposit = (int)resMap.get("deposit");
					Long vid = (Long)resMap.get("id");	
					
					if ( (degree + deposit) < i )
					{
						isDelete = true;
						// remove nodes and edges
						auxdDB.execute(delRelation);
						auxdDB.execute(delNode);
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
		
		return kcMap;
	}
	
	public static Map<String,Object> getNodeProperties(Long id) {
		Map<String,Object> par = new HashMap<>();
		par.put("id",id);
		Result res = graphDB.execute("MATCH (v) "
				+ "WITH size((v)--()) as d, coalesce(v.KClassUpperBound,v.KClass) as k, v.deposit as p "
				+ "WHERE ID(v) = $id "
				+ "RETURN k,p,d", par);
		Map<String,Object> resMap = new HashMap<>(res.next());
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
				+ "WITH coalesce(v.KClass,v.KClassUpperBound) as k "
				+ "RETURN k");
		long max = Long.MIN_VALUE;
		while(res.hasNext()) {
			long k = (Long)res.next().get("k");
			if(max < k)
				max = k;
		}
		return max;
	}

}
