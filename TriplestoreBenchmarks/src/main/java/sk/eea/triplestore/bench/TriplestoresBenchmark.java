package sk.eea.triplestore.bench;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.stores.AbstractSailStore;
import sk.eea.triplestore.bench.stores.Store;

public class TriplestoresBenchmark {

	private static Logger logger = LoggerFactory
			.getLogger(TriplestoresBenchmark.class);
	protected Settings settings;
	protected Map<String, Double> results = new LinkedHashMap<>();

	protected int file_count;
	protected int sparql_count;
	protected String testId;

	public Store getNextTest() throws Exception {
		if (testId == null || testId.isEmpty()) {
			throw new Exception("Specify -Dtests.run.test=<ID>");
		}
		String repositoryType = settings.getValue("test." + testId
				+ ".repository.type");
		Store store;

		store = (Store) AbstractSailStore.class.getClassLoader()
				.loadClass(repositoryType).newInstance();
		store.initialize(settings, testId);
		return store;
	}

	/**
	 * @throws IOException
	 * @throws FileNotFoundException
	 * 
	 */
	public TriplestoresBenchmark() throws FileNotFoundException, IOException {
		settings = new Settings();

		testId = settings.getValue("tests.run.test");

		file_count = Integer.parseInt(settings.getValue("import.files.count"));
		sparql_count = Integer
				.parseInt(settings.getValue("sparql.query.count"));

	}

	private void addResult(String testPhase, Double value) {
		results.put(testPhase, value);
	}

	/**
	 * 
	 */
	public void runTests() {

		try {
			Store p = getNextTest();

			// load test
			p.clearDataBeforeRun(true);
			p.clearDataAfterRun(false);
			long[] ret = p.testLoadData(settings.getFileName(1),
					settings.getFileUri(1));
			addResult("LOAD_1", (double) ret[0]);
			p.clearDataBeforeRun(false);
			for (int i = 2; i <= file_count; i++) {
				p.testLoadData(settings.getFileName(i), settings.getFileUri(i));
			}

			// batch load
			int[] commitSizes = { 1000, 10000 };
			for (int commitSize : commitSizes) {
//				ret = p.testLoadDataBatch(settings.getFileName(1),
//						settings.getFileUri(1), commitSize);
				addResult("LOAD_1_" + commitSize, 0.0);
			}
//			p.clearDataBeforeRun(false);
//			p.clearDataAfterRun(false);
//			ret = p.testLoadDataBatch(settings.getFileName(1),
//					settings.getFileUri(1), 100000);
			addResult("LOAD_1_100000", 0.0);

			// load all data + sparql test
			for (int i = 2; i <= file_count; i++) {
//				ret = p.testLoadDataBatch(settings.getFileName(i),
//						settings.getFileUri(i), 50000);
				addResult("LOAD_" + i + "_50000", 0.0);
			}

			for (int i = 1; i <= sparql_count; i++) {
				ret = p.testSparql(settings.getSparqlQuery(i));
				addResult("SPARQL_" + i, (double) ret[0]);
			}

			p.shutDown();
			writeResults(results);
		} catch (Exception e) {
			logger.error("error:", e);
		} finally {

		}

	}

	public void writeResults(Map<String, Double> results) throws Exception {
		// write results
		String outputFilename = settings.getValue("test." + testId
				+ ".output.file");
		FileWriter outputWriter = new FileWriter(outputFilename, true);
		outputWriter.append("=multi");
		outputWriter.append(System.lineSeparator());
		for (String phase : results.keySet()) {
			outputWriter.append(StringUtils.rightPad(phase, 30));
			outputWriter.append(String.valueOf(results.get(phase)));
			outputWriter.append(System.lineSeparator());
		}
		outputWriter.close();
	}

	/**
	 * main
	 * 
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		TriplestoresBenchmark s = new TriplestoresBenchmark();
		s.runTests();
	}

}
