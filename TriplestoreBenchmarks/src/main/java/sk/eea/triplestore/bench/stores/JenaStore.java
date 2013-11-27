package sk.eea.triplestore.bench.stores;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.Settings;

public class JenaStore implements Store {

	private static final Logger logger = LoggerFactory.getLogger(JenaStore.class);
	
	private Model model;
	
	public void initialize(Settings settings) throws Exception {
	    model = ModelFactory.createDefaultModel();
	}

	public void shutDown() throws Exception {
		model.close();
	}

	public String getDescription() {
		return "ApacheJena";
	}


	public long[] testLoadData(String fileName, String uri) throws Exception {
		 InputStream in = new FileInputStream(fileName);                                                         
         model.read(in, null, "RDFXML");
	}

	public long[] testLoadDataBatch(String fileName, String uri, int batchSize) throws Exception {
		return new long[]{0L,0L};
	}

	public long[] testSparql(String sparql) throws Exception {
		long count = 0L;
		long time = 0L;
		
		long start = System.currentTimeMillis();
		Query query = QueryFactory.create(sparql);
        ARQ.getContext().setTrue(ARQ.useSAX);        
        QueryExecution qexec = QueryExecutionFactory.create(query, model);
        ResultSet results = qexec.execSelect();
        while (results.hasNext()) {
        	int count++;
        }
        time = System.currentTimeMillis() - start;
        qexec.close();
        
		logger.info("done, SPARQL returned: " + count + ", time: " + time + " ms");

        return new long[] {time, count};
	}

	public void clearDataBeforeRun(boolean clear) {
	}

}
