import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
	
	// Implement by Li
	public static void computeKClassUpperBound(int KClass, int limit)
	{
		
	}
	
	// Implement by Pansa
	public static ArrayList<Integer> UpperBoundReduction(ArrayList<Integer> inputList)
	{
		ArrayList<Integer> rtnList = new ArrayList<Integer>();
		
		// TO DO
		
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
		computeKClassUpperBound(KClass, limit);
		// Terminate the Graph database service
		closeDB();
		
		System.out.println("Job Completed!");
	}

}
