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
import java.util.TreeMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.graphdb.RelationshipType;

public class DMP {
	// Public members
	public static GraphDatabaseService graphDB;
	public static Transaction tx ;
	
	public static void createDB(String neo4jFolder)
	{
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neo4jFolder))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
		tx = graphDB.beginTx();
	}	
	
	public static void closeDB()
	{
		tx.close();
		graphDB.shutdown();
	}
	
	public static HashMap<Long, Double> computeRank(HashMap<Long, ArrayList<Long>> hMap)
	{
		HashMap<Long, Double> rankMap = new HashMap<Long, Double>();
		
		TreeMap<Long, ArrayList<Long>> sorted = new TreeMap<Long, ArrayList<Long>>(); 
		sorted.putAll(hMap);
						
		int curRank = 0;
		for (Map.Entry<Long, ArrayList<Long>> entry : sorted.entrySet())  
		{
			ArrayList<Long> valList = entry.getValue();
			int len = valList.size();
			int sum  = len * (len + 1) / 2;
			sum += len * curRank;
			double rankVal = (double)sum / (double)len;
			curRank += len;
			
			for (Long val : valList)
			{
				rankMap.put(val, rankVal);
			}
		}
		
		return rankMap;
	}
	
	// Computer the DMP value for each vertex and store them into the Neo4j database
	public static void computeDMP()
	{
		// Get K and D for each Vertex
		Result res = graphDB.execute("MATCH (v) RETURN ID(v) as id, v.degree as d, coalesce(v.KClass,v.KClassUpperBound) as k");
		
		HashMap<Long, ArrayList<Long>> dMap = new HashMap<Long, ArrayList<Long>>();
		HashMap<Long, ArrayList<Long>> kMap = new HashMap<Long, ArrayList<Long>>();
		ArrayList<Long> vList = new ArrayList<Long>();
		
		long vid, d, k;
		
		while(res.hasNext()) 
		{
			Map<String,Object> resMap = res.next();
			vid = (Long)resMap.get("id");
			d = (Long)resMap.get("d");
			k = (Long)resMap.get("k");
			
			ArrayList<Long> dList;
			ArrayList<Long> kList;
			
			if (dMap.containsKey(d))
			{
				dList = new ArrayList<Long>(dMap.get(d));
			}
			else
			{
				dList = new ArrayList<Long>();
			}
			
			if(kMap.containsKey(k))
			{
				kList = new ArrayList<Long>(kMap.get(k));
			}
			else
			{
				kList = new ArrayList<Long>();
			}
			
			dList.add(vid);
			kList.add(vid);
			
			// Insert d and k in an array list
			dMap.put(d, dList);
			kMap.put(k, kList);
			
			vList.add(vid);
		}
				
		// Compute the rank for Degree and K
		HashMap<Long, Double> dRank = computeRank(dMap);
		HashMap<Long, Double> kRank = computeRank(kMap);
		
		// Insert to the database
		HashMap<String, Object> parameter = new HashMap<String, Object>();
		for(Long v : vList)
		{
			parameter.clear();
			double dmp = Math.abs( Math.log10(dRank.get(v)) - Math.log10(kRank.get(v)) );
			parameter.put("id", v);
			parameter.put("DMP", dmp);
			graphDB.execute("MATCH (v) WHERE ID(v)=$id SET v.DMP=$DMP", parameter);
		}
	}
	
	public static void checkDMP()
	{
		System.out.println("Check the first 10 DMP value");
		Result res = graphDB.execute("MATCH (v) RETURN ID(v) as id, v.DMP as DMP LIMIT 10");
		while(res.hasNext()) 
		{
			Map<String,Object> resMap = res.next();
			long vid = (Long)resMap.get("id");
			double dmp = (Double)resMap.get("DMP");
			
			System.out.println("vid:" + vid + ", " + "dmp:" + dmp);
		}
	}
	
	// Main Function
	public static void main(String[] args) 
	{
		// Only Takes in the Graph Database path
		System.out.println("Job Started!");
		
		String neo4jPath = args[0];
		createDB(neo4jPath);
		computeDMP();
		checkDMP();
		closeDB();
		
		System.out.println("Job Completed!");
	}

}
