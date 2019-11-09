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
			
			ArrayList<Long> dList, kList;
			
			if (dMap.containsKey(vid))
			{
				dList = dMap.get(vid);
				kList = kMap.get(vid);
			}
			else
			{
				dList = new ArrayList<Long>();
				kList = new ArrayList<Long>();
			}
			
			dList.add(d);
			kList.add(k);
			
			// Insert d and k in an array list
			dMap.put(vid, dList);
			kMap.put(vid, kList);
			
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
	
	// Main Function
	public static void main(String[] args) 
	{
		// Only Takes in the Graph Database path
		String neo4jPath = args[0];
		createDB(neo4jPath);
		computeDMP();
		closeDB();
	}

}
