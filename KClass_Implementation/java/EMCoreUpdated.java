import java.io.File;
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

public class EMCore {

	// Public members
	public static GraphDatabaseService graphDB;
	
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
		graphDB.shutdown();
	}
	
	public static ArrayList<Long> getListSortedByVal(HashMap<Long, Integer> hm)
	{
		 List<Map.Entry<Long, Integer> > list = new LinkedList<Map.Entry<Long, Integer> >(hm.entrySet()); 
		 Collections.sort(list, new Comparator<Map.Entry<Long, Integer> >() 
		 { 
			 @Override
			 public int compare(Entry<Long, Integer> arg0, Entry<Long, Integer> arg1)
			 {
				 // TODO Auto-generated method stub
				 return (arg0.getValue()).compareTo(arg1.getValue()); 
			 } 
		 }); 
		 
		 ArrayList<Long> rtnList = new ArrayList<Long>();
		 
		 for (Map.Entry<Long, Integer> item : list) 
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
		
		HashMap<Long, Integer> kcupMap = new HashMap<Long, Integer>();
				
		// Stop the process the number of iteration hits the given limit
		while (iter < limit )
		{
			// Clear the map
			kcupMap.clear();
			
			// foreach v in the input list
			for(Long v : inputList)
			{
				// Get KClass Upper Bound from vertex map
				// See description for class variables
				kcupMap.put(v, vertexMap.get(v).get(2));
			}
			
			inputList = getListSortedByVal(kcupMap);
			
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
		// TODO
		for(Long v : inputList) {
			ArrayList<Long> Z = new ArrayList<>();
			Map<String,Object> par = new HashMap<>();
			par.put("id",v);
			par.put("d", vertexMap.get(v).get(2));
			
			// Get degree of neighbors of v
			Result res = graphDB.execute("MATCH (v)--(v1) "
					+ "WITH ID(v1) AS id, size((v1)--()) AS degree "
	    			+ "WHERE id = $id AND degree > $d "
	    			+ "RETURN id, degree "
	    			+ "ORDER BY degree ASC",par);
			while(res.hasNext()) {
				Map<String,Object> resMap = res.next();
				Z.add((Long)resMap.get("id"));
			}
			
			int degree_v = vertexMap.get(v).get(3);
			int kcub_v = vertexMap.get(v).get(2);
			if(degree_v - Z.size() < kcub_v) {
				int min = Integer.MAX_VALUE;
				for(int i = 1; i <= Z.size(); i++) {
					int kcub_u = vertexMap.get(Z.get(i-1)).get(2);
					int c = Math.max(degree_v-i, kcub_u);
					if(c < min)
						min = c;
				}
				vertexMap.get(v).set(2, min);
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
		String srcNeo4jFolder = args[0];
		String auxNeo4jFolder= args[1];
		int KClass = Integer.parseInt(args[2]);
		int limit = Integer.parseInt(args[3]);
		float p = Float.parseFloat(args[4]);
		int stepover = Integer.parseInt(args[5]);
		
		// Create the graph database with the given source Neo4j path
		createDB(srcNeo4jFolder);
		// K-Class upper bound estimation
		// computeKClassUpperBound(KClass, limit);
		// Terminate the Graph database service
		closeDB();
		/** Test Block
		createDB("/home/sapan/Documents/CSCI-723-GraphDB/Assignment7/TryGraphs");
		KCoreDecomposition(6, 2, 3);
		System.out.println(vertexMap);
		System.out.println("Job Completed!");
		closeDB();
		**/

		
	}
	
	// To be implemented by Sapan
	public static void KCoreDecomposition(int mink, int limit, int step) {
		Result res = graphDB.execute("MATCH (v) "
				+ "WITH ID(v) AS id, LABELS(v) as label, v.KClass as kclass, v.KClassUpperBound as kcub, v.OriginalId as oid "
				+ "RETURN id, coalesce(kclass, kcub) as degree, oid");
		ArrayList<Long> W = new ArrayList<>();
		
		// vertex to list of properties 
		vertexMap = new HashMap<>();
		// vertex to original id
		originalIdMap = new HashMap<>();
		
		while(res.hasNext()) {
			// reference to query result
			Map<String,Object> resMap = res.next();
			// To store vertex properties in order deposit, kclass, kclass upper bound, degree
			ArrayList<Integer> vertexProperties = new ArrayList<>();
			vertexProperties.add(0);
			int degree = (int)resMap.get("degree");
			Long id = (Long)resMap.get("id");
			Long oid = (Long)resMap.get("oid");
			if(degree == 1) {
				// this condition is true when no k class upper bound is found
				// store 1 for kclass and 0 for kclass upper bound
				vertexProperties.add(1);
				vertexProperties.add(0);
			}
			else {
				// when kclass upper bound exists
				vertexProperties.add(0);
				vertexProperties.add(degree);
				W.add(id);
			}
			vertexProperties.add(degree);
			originalIdMap.put(id, oid);
			vertexMap.put(id,vertexProperties);
		}
		//System.out.println(W);
		refinedUpperBound(W,limit);
		
		// TODO rest of the code
		/**System.out.println(W);
		int Ku = Integer.MAX_VALUE;
		do {
			Ku = Math.max();
		}while(Ku > mink);**/
	}
	
	// To be implemented by Li
	public static Map<Long, Integer> ComputeCore(GraphDatabaseService auxdDB, int Kl, int Ku)
	{
		Map<Long, Integer> kcMap = new HashMap<Long, Integer>();
		String query = "MATCH (v) RETURN ID(v) AS id, v.deposit AS deposit, v.degree AS degree";
		String delRelation = "MATCH (n)-[r]-() WITH ID(v) AS id WHERE id = $id DELETE r";
		String delNode = "MATCH (n) WITH ID(v) AS id WHERE id = $id DELETE n";
		
		for (int i = Kl; i <= Ku; i++)
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

}
