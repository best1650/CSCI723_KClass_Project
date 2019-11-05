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
	public static void computeKClassUpperBound(ArrayList<Long> inputList, int limit)
	{
		// Iterator counter
		int iter = 0;
		
		// Get KClass Upper bound Map for the input vList
		String query = "MATCH (v) WITH ID(v) as id WHERE id = $id RETURN v.KClassUpperBound AS KCUB";
		HashMap<String, Object> parameter = new HashMap<String, Object>();
		HashMap<Long, Long> kcupMap = new HashMap<Long, Long>();
		ArrayList<Long> curList = new ArrayList<Long> (inputList);
				
		// Stop the process the number of iteration hits the given limit
		while (iter < limit )
		{
			// Clear the map
			kcupMap.clear();
			
			// foreach v in the input list
			for(Long v : curList)
			{
				parameter.put("id", v);
				Result result = graphDB.execute(query, parameter);
				
				// Add v's corresponding kClass cup bound to the map
				while(result.hasNext()) 
				{
					Map<String,Object> resMap = result.next();
					kcupMap.put(v, (Long)resMap.get("KCUB"));
				}
			}
			
			curList = getListSortedByVal(kcupMap);
			
			// Create an empty list
			ArrayList<Long> nextList = UpperBoundReduction(curList);
			
			// For next iteration
			curList = new ArrayList<Long>(nextList);
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
			
			// TODO get upper bound of v
			par.put("d", 3);
			Result res = graphDB.execute("MATCH (v)--(v1) "
					+ "WITH ID(v1) AS id, size((v1)--()) AS degree "
	    			+ "WHERE id = $id AND degree>=$d "
	    			+ "RETURN id, degree "
	    			+ "ORDER BY degree ASC",par);
			while(res.hasNext()) {
				Map<String,Object> resMap = res.next();
//				System.out.println(resMap);
				Z.add((Long)resMap.get("id"));
			}
			res = graphDB.execute("MATCH (v) "
					+ "WITH ID(v) AS id, size((v)--()) AS degree, v.KClassUpperBound as kcup, v.KClass as kclass "
	    			+ "WHERE id = $id "
	    			+ "RETURN id, coalesce(kcup,kclass) as k, degree ",par);
			Map<String,Object> resMap = res.next();
			int degree = (int)resMap.get("degree");
			int k = (int)resMap.get("k");
			if(degree - Z.size() < k) {
				int min = Integer.MAX_VALUE;
				for(int i = 1; i <= Z.size(); i++) {
					par.put("id", Z.get(i-1));
					res = graphDB.execute("MATCH (v) "
							+ "WITH ID(v) AS id, v.KClassUpperBound as kcup, v.KClass as kclass "
			    			+ "WHERE id = $id "
			    			+ "RETURN id, coalesce(kcup,kclass) as k",par);
					int c = Math.max(degree-i, (int)res.next().get("k"));
					if(c < min)
						min = c;
				}
				k = min;
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
		
		System.out.println("Job Completed!");
	}
	
	// To be implemented by Sapan
	public static void KCoreDecomposition(int mink, int limit, int step) {
		
	}
	
	// To be implemented by Li
	public static void ComputeCore(int Kl, int Ku) {
		
	}

}
