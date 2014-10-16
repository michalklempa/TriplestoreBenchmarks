package sk.eea.triplestore.bench.stores;

import java.util.Properties;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.openrdf.repository.sail.SailRepository;

import sk.eea.triplestore.bench.Settings;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;

public class Neo4jStore extends AbstractSailStore {

    @Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
//        Map<String, String> config = new HashMap<String, String>();
//        config.put("neostore.nodestore.db.mapped_memory", "1000M");
//        config.put("neostore.propertystore.db.mapped_memory", "1000M");
//        config.put("neostore.propertystore.db.strings.mapped_memory", "1000M");
//        config.put("neostore.propertystore.db.arrays.mapped_memory", "1000M");
//        config.put("neostore.relationshipstore.db.mapped_memory", "1000M");
//        GraphDatabaseService dbService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(settings.repository_url).setConfig(config).newGraphDatabase();
        
        GraphDatabaseService dbService = new GraphDatabaseFactory().newEmbeddedDatabase(repositoryUrl);
        Neo4jGraph neo4jGraph = new Neo4jGraph(dbService, true);
        GraphSail<KeyIndexableGraph> sail = new GraphSail<KeyIndexableGraph>(neo4jGraph);
        sail.initialize();
        repository = new SailRepository(sail);
        /*Neo4jGraph graph = new Neo4jGraph(settings.repository_url);
        GraphSail<KeyIndexableGraph> sail = new GraphSail<KeyIndexableGraph>(graph);
        sail.initialize();*/
    }


}
