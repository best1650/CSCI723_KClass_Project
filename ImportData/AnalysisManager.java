import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class AnalysisManager {

	public static BatchInserter inserter;
	
	public static void initGraphDatabase(String neo4jDb)
	{
		try 
		{
			inserter = BatchInserters.inserter(new File(neo4jDb));
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void closeGraphDatabase()
	{
		inserter.shutdown();
	}
	
	public static void importDataFromFiles(String srcDataPath)
	{
		File folder = new File(srcDataPath);
		String fileName;
		long offset = 0;
		
		for (File fEntry : folder.listFiles())
		{
			if(fEntry.isFile())
			{
				fileName = fEntry.getName();
				offset = getGraphDataFromFile(fEntry.getAbsolutePath(), fileName, offset);
			}
		}
	}
	
	public static long getGraphDataFromFile(String filePath, String fileName, long offset)
	{
		long curNodeId = -1;
		int degree = 0;
		String st;
		Map<String, Object> attMap = new HashMap<String, Object>();
		HashSet<Long> nodeList = new HashSet<Long>();
		
		try 
		{
			File file = new File(filePath); 
			BufferedReader br = new BufferedReader(new FileReader(file)); 
			
			while ((st = br.readLine()) != null) 
			{
				if(st.contains("#"))
				{
					continue;
				}
				
				String[] nodeInfo = st.split("\t");
				long FromNodeId = Long.valueOf(nodeInfo[0]);
				long ToNodeId = Long.valueOf(nodeInfo[1]);
				
				nodeList.add(FromNodeId);
				nodeList.add(ToNodeId);
			}
			
			for (Long node : nodeList)
			{
				inserter.createNode((offset + node), attMap, Label.label(fileName));
			}
			
			br.close();
			br = new BufferedReader(new FileReader(file));
			
			while ((st = br.readLine()) != null) 
			{
				if(st.contains("#"))
				{
					continue;
				}
				
				String[] nodeInfo = st.split("\t");
				long FromNodeId = Long.valueOf(nodeInfo[0]);
				long ToNodeId = Long.valueOf(nodeInfo[1]);
				
				if(fileName.compareTo("Email-Enron.txt") != 0 || FromNodeId > ToNodeId)
				{
					inserter.createRelationship(
							(offset + FromNodeId), 
							(offset + ToNodeId), 
							RelationshipType.withName(fileName), 
							null
					);
				}
				
				if (curNodeId != FromNodeId)
				{
					if(curNodeId != -1)
					{
						attMap.clear();
						attMap.put("origID", curNodeId);
						attMap.put("KClass", degree);
						attMap.put("KClassUpperBound", 0);
						inserter.setNodeProperties((offset + curNodeId), attMap);
						degree = 0;
					}
				}
				
				degree++;
				curNodeId = FromNodeId;
			}
			
			attMap.clear();
			attMap.put("origID", curNodeId);
			if(degree == 1)
			{
				attMap.put("KClass", degree);
				attMap.put("KClassUpperBound", 0);
			}
			else
			{
				attMap.put("KClass", 0);
				attMap.put("KClassUpperBound", degree);				
			}
			inserter.setNodeProperties((offset + curNodeId), attMap);

			br.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return (offset + Collections.max(nodeList) + 1);
	}
	
	public static void getGraphStat(String neo4jDBPath, String srcDataPath)
	{
		GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(new File(neo4jDBPath))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
		
		File folder = new File(srcDataPath);
		for (File fEntry : folder.listFiles())
		{
			if(fEntry.isFile())
			{
				String fileName = fEntry.getName();
				System.out.println(fileName);
				String query =
						"MATCH(n1:`" + fileName + "`)" + 
						" RETURN n1.origID, size((n1)--()), n1.KClass LIMIT 10";
				runQuery(query, graphDb);
				System.out.println();
			}
		}
		
		graphDb.shutdown();
	}
	
	public static void runQuery(String query, GraphDatabaseService graphDb)
	{
		try
		{			
			Transaction ignored = graphDb.beginTx();
			Result result = graphDb.execute(query);
			int count = 0;
			while(result.hasNext())
			{
				count++;
				System.out.print(count + ". ");
				for (Map.Entry<String,Object> entry : result.next().entrySet())
				{
					System.out.print(entry.getKey() + ":" + entry.getValue().toString() + "\t");
				}
				 
				System.out.println();
			}
		}
		catch(Exception e)
		{
			
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String srcDataPath = args[0];
		String neo4jDBPath = args[1];
		
		boolean importData = false;
		
		if (importData)
		{
			initGraphDatabase(neo4jDBPath);
			importDataFromFiles(srcDataPath);
			closeGraphDatabase();
		}
		else
		{
			getGraphStat(neo4jDBPath, srcDataPath);
		}

		System.out.println("Completed!");
	}

}
