package sk.eea.triplestore.bench;


import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.stores.BigdataStore;
import sk.eea.triplestore.bench.stores.OpenRdfStore;
import sk.eea.triplestore.bench.stores.OwlimStore;
import sk.eea.triplestore.bench.stores.Store;
import sk.eea.triplestore.bench.stores.VirtuosoStore;


public class TriplestoresBenchmark {
		
	private static Logger logger = LoggerFactory.getLogger(TriplestoresBenchmark.class);

	/**
	 * 
	 */
	public TriplestoresBenchmark() {
	}

	private Store getProvider(Settings settings) {
		if ("openrdf".equals(settings.repository_type)) {
			return new OpenRdfStore();
		} else if ("virtuoso".equals(settings.repository_type)) {
			return new VirtuosoStore();
		} else if ("bigdata".equals(settings.repository_type)) {
			return new BigdataStore();
		} else if ("owlim".equals(settings.repository_type)) {
			return new OwlimStore();
		}
		throw new IllegalArgumentException("invalid provider:" + settings.repository_type);
	}


	private void addResult(Map<String, List<Long>> results,  String testName, long time) {
		List<Long> times = results.get(testName);
		if (times == null) {
			times  = new ArrayList<Long>();
			results.put(testName, times);
		}
		times.add(time);
		
	}
	
	/**
	 * 
	 */
	public void runTests() {

		try {
			Settings settings = new Settings();
			settings.loadSettings();
			
			Store p = getProvider(settings);
			p.initialize(settings);
	
			Map<String, List<Long>> results = new LinkedHashMap<String, List<Long>>();
			for (int testRun = 0; testRun < settings.runs; testRun++) {

				//load test
				long[] ret = p.testLoadData(settings.getFileName(1), settings.getFileUri(1));
				addResult(results, "Load (count:" + ret[1] + ")", ret[0]);
				
				//batch load
				int[] commitSizes = { 1000, 10000, 100000};
				for (int commitSize : commitSizes) {
					ret = p.testLoadDataBatch(settings.getFileName(1), settings.getFileUri(1), commitSize);
					addResult(results, "LoadInBatch (commit size:" + commitSize + " count:" + ret[1] + ")", ret[0]);
				}

				//load all data + sparql test
				p.clearDataBeforeRun(false);
					for (int i = 1; i <= settings.file_count; i++) {
					p.testLoadDataBatch(settings.getFileName(i), settings.getFileUri(i), 50000);
				}
				p.clearDataBeforeRun(true);

				for (int i = 1; i <= settings.sparql_count; i++) {
					ret = p.testSparql(settings.getSparqlQuery(i));
					addResult(results, "SPARQL " + i + "(count:" + ret[1] + ")", ret[0]);
				}
			
			}
			p.shutDown();
			
			writeResults(settings, results);
			
		} catch (Exception e) {
			logger.error("error:", e);
		} finally {
			
		}

	}
	
	private void writeResults(Settings settings, Map<String, List<Long>> results) throws Exception {
		//header
		FileWriter writer = new FileWriter(settings.output);
		StringBuilder header = new StringBuilder("test name");
		for (int i = 1; i <= settings.runs; i++) {
			header.append(",run " + i + " in ms");
		}
		header.append(System.lineSeparator());
		writer.write(header.toString());
		
		//write results
		for (String result : results.keySet()) {
			writer.append(result);
			for (Long time : results.get(result)) {
				writer.append("," + time.toString());
			}
			writer.append(System.lineSeparator());
		}
		writer.flush();
		writer.close();
		
	}

	/**
	 * main
	 * @param args
	 */
	public static void main(String[] args) {
		TriplestoresBenchmark s = new TriplestoresBenchmark();
		s.runTests();
	}

}
