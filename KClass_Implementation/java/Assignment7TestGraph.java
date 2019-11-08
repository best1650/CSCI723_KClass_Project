import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Assignment7TestGraph {
	public static void main(String args[]) {
		BatchInserter batchInserter = null;
		try {
			batchInserter = BatchInserters.inserter(new File(args[0]));
			
			// node ids
			long ida = 0l,idb = 1l,idc = 2l,idd = 3l,ide = 4l,idf = 5l,idg = 6l,idh = 7l,idi = 8l, idl = 9l, idk = 10l, idj = 11l,
					idm = 12l, idn = 13l;
			
			// For node properties
    		Map<String,Object> n = new HashMap<>();
    		
    		// Create nodes
//    		n.put("KClass", 1);
//    		n.put("KClassUpperBound", 0);
			batchInserter.createNode(ida,n, Label.label("a"));
//    		n = new HashMap<>();

//    		n.put("KClassUpperBound", 3);
    		batchInserter.createNode(idb,n, Label.label("b"));
//    		n = new HashMap<>();

//    		n.put("KClass", 1);
    		//n.put("KClassUpperBound", 0);
    		batchInserter.createNode(idc,n, Label.label("c"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 3);
    		batchInserter.createNode(idd,n, Label.label("d"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 4);
    		batchInserter.createNode(ide,n, Label.label("e"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 2);
    		batchInserter.createNode(idf,n, Label.label("f"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 3);
    		batchInserter.createNode(idg,n, Label.label("g"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 5);
    		batchInserter.createNode(idh,n, Label.label("h"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 6);
    		batchInserter.createNode(idi,n, Label.label("i"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 5);
    		batchInserter.createNode(idl,n, Label.label("l"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 4);
    		batchInserter.createNode(idk,n, Label.label("k"));
//    		n = new HashMap<>();

//    		n.put("KClass", 1);
    		//n.put("KClassUpperBound", 0);
    		batchInserter.createNode(idj,n, Label.label("j"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 2);
    		batchInserter.createNode(idm,n, Label.label("m"));
//    		n = new HashMap<>();

    		//n.put("KClass", 0);
//    		n.put("KClassUpperBound", 4);
    		batchInserter.createNode(idn,n, Label.label("n"));
    		
    		
    		// Create relations
    		
			batchInserter.createRelationship(ida, idb, RelationshipType.withName(""), new HashMap<>());
			
			batchInserter.createRelationship(idb, idc, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idb, idd, RelationshipType.withName(""), new HashMap<>());

			batchInserter.createRelationship(idd, ide, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idd, idg, RelationshipType.withName(""), new HashMap<>());

			batchInserter.createRelationship(ide, idf, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(ide, idh, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(ide, idi, RelationshipType.withName(""), new HashMap<>());

			batchInserter.createRelationship(idf, idi, RelationshipType.withName(""), new HashMap<>());
			
			batchInserter.createRelationship(idg, idh, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idg, idl, RelationshipType.withName(""), new HashMap<>());
			
			batchInserter.createRelationship(idh, idl, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idh, idk, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idh, idi, RelationshipType.withName(""), new HashMap<>());
			
			batchInserter.createRelationship(idi, idj, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idi, idk, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idi, idn, RelationshipType.withName(""), new HashMap<>());

			batchInserter.createRelationship(idl, idm, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idl, idn, RelationshipType.withName(""), new HashMap<>());
			batchInserter.createRelationship(idl, idk, RelationshipType.withName(""), new HashMap<>());
			
			batchInserter.createRelationship(idk, idn, RelationshipType.withName(""), new HashMap<>());
			
			batchInserter.createRelationship(idm, idn, RelationshipType.withName(""), new HashMap<>());

		}catch(IOException e) {
			e.printStackTrace();
		}finally {
			batchInserter.shutdown();
		}

	}

}
