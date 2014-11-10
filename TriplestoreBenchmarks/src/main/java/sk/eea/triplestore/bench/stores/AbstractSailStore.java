package sk.eea.triplestore.bench.stores;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.Settings;

public abstract class AbstractSailStore implements Store {

	protected Repository repository;
	protected boolean clearDataBeforeRun = true;
	protected boolean clearDataAfterRun = true;
	protected String testId;
	protected String outputFile;
	protected String repositoryType;
	protected String repositoryId;
	protected String repositoryUrl;
	protected String testDescription;
	protected String startupCommand;
	protected String shutdownCommand;

	public AbstractSailStore() {
		super();
	}

	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		this.testId = testId;
		outputFile = settings.getValue("test." + testId + ".output.file");
		repositoryType = settings.getValue("test." + testId
				+ ".repository.type");
		repositoryId = settings.getValue("test." + testId + ".repository.id");
		repositoryUrl = settings.getValue("test." + testId + ".repository.url");

		startupCommand = settings.getValue("test." + testId
				+ ".startup.command");
		shutdownCommand = settings.getValue("test." + testId
				+ ".shutdown.command");

		testDescription = settings.getValue("test." + testId + ".description");
		if (null != startupCommand && !startupCommand.isEmpty()) {
			CommandLine cmdLine = CommandLine.parse(startupCommand);
			DefaultExecutor executor = new DefaultExecutor();
			executor.execute(cmdLine);
		}
	}

	protected void clearDataBeforeRun(RepositoryConnection connection,
			Resource... resource) throws Exception {
		if (clearDataBeforeRun) {
			connection.clear(resource);
		}
	}

	protected void clearDataAfterRun(RepositoryConnection connection,
			Resource... resource) throws Exception {
		if (clearDataAfterRun) {
			connection.clear(resource);
		}
	}

	/**
	 * 
	 */
	public long[] testLoadData(String fileName, String uri) throws Exception {
		File inputFile = new File(fileName);
		getLogger().info(
				"########## test loadRDF, file: " + inputFile.getName()
						+ " ##########");

		long time = 0L;
		long count = 0L;

		Resource context = new URIImpl(uri); // nejde pre bigdata, musi byt null

		RepositoryConnection conn1 = repository.getConnection();
		clearDataBeforeRun(conn1, context);
		conn1.close();

		RepositoryConnection conn = repository.getConnection();
		try {
			long start = System.currentTimeMillis();
			conn.add(inputFile, uri, Rio.getParserFormatForFileName(fileName),
					context);
			time = System.currentTimeMillis() - start;
			count = -1L;
			count = conn.size(context); // pre owlim spadne
			getLogger().info(
					"done, RDF count: " + count + ", time: " + time + " ms");
			clearDataAfterRun(conn, context);
		} finally {
			conn.close();
		}

		return new long[] { time, count };
	}

	/**
	 * 
	 */
	public long[] testLoadDataBatch(String fileName, String uri, int batchSize)
			throws Exception {
		Resource context = new URIImpl(uri);

		File inputFile = new File(fileName);
		getLogger().info(
				"########## test loadRDFInBatch, file: " + inputFile.getName()
						+ ", batchSize: " + batchSize + "  ##############");
		long time = 0L;
		long count = 0L;

		RepositoryConnection conn1 = repository.getConnection();
		clearDataBeforeRun(conn1, context);
		conn1.close();

		RepositoryConnection conn = repository.getConnection();
		CancellableCommitSizeInserter rdfInserter = new CancellableCommitSizeInserter(
				conn, batchSize);
		RDFFormat format = Rio.getParserFormatForFileName(inputFile
				.getCanonicalPath());
		RDFLoader loader = new RDFLoader(conn.getParserConfig(),
				conn.getValueFactory());
		try {
			long start = System.currentTimeMillis();
			loader.load(inputFile, uri, format, rdfInserter);
			time = System.currentTimeMillis() - start;
			count = rdfInserter.getRealStatementCounter();
			getLogger().info(
					"done, RDF count: " + count + ", time: " + time + " ms");
			clearDataAfterRun(conn, context);
		} finally {
			conn.close();
		}

		return new long[] { time, count };
	}

	/**
	 * 
	 */
	public long[] testSparql(String sparql) throws Exception {
		getLogger().info("########## test SPARQLQuery ##############");
		RepositoryConnection conn = repository.getConnection();

		long time = 0L;
		long count = 0;

		try {
			long start = System.currentTimeMillis();
			TupleQuery tupleQuery = conn.prepareTupleQuery(
					QueryLanguage.SPARQL, sparql);
			TupleQueryResult result = tupleQuery.evaluate();
			try {
				while (result.hasNext()) {
					result.next();
					count++;
				}
				time = System.currentTimeMillis() - start;
				getLogger().info(
						"done, SPARQL returned: " + count + ", time: " + time
								+ " ms");
			} finally {
				result.close();
			}
		} finally {
			conn.close();
		}
		return new long[] { time, count };
	}

	public long[] testInsert(List<String> uris) throws Exception {
		getLogger().info("########## test Insert Query ##############");
		RepositoryConnection conn = repository.getConnection();

		long time = 0L;
		long count = 0;

		try {
			 List<URIImpl> urisList = new ArrayList<URIImpl>();
			for(String uri:uris) {
				urisList.add(new URIImpl(uri));
			}
			getLogger().info("Starting inserting: " + conn.size(urisList.toArray(new URIImpl[0])));
			String sparql = "INSERT { ?s ?p ?o } WHERE { ?s ?p ?o }";
			Update update = conn.prepareUpdate(QueryLanguage.SPARQL, sparql);
			DatasetImpl dataset = new DatasetImpl();
			for (String uri : uris) {
				dataset.addDefaultGraph(new URIImpl(uri));
				dataset.addNamedGraph(new URIImpl(uri));
			}
			dataset.setDefaultInsertGraph(new URIImpl("http://newInsertGraph"));
			dataset.addDefaultRemoveGraph(new URIImpl("http://newInsertGraph"));
			update.setDataset(dataset);

			long start = System.currentTimeMillis();
			update.execute();
			time = System.currentTimeMillis() - start;
			count = conn.size(new URIImpl("http://newInsertGraph"));
			getLogger().info(
					"done, INSERT inserted: " + count + ", time: " + time
							+ " ms");
		} finally {
			conn.close();
		}
		return new long[] { time, count };
	}

	/**
	 * 
	 */
	public void shutDown() throws Exception {
		if (repository != null) {
			repository.shutDown();
		}
		if (null != shutdownCommand && !shutdownCommand.isEmpty()) {
			CommandLine cmdLine = CommandLine.parse(shutdownCommand);
			DefaultExecutor executor = new DefaultExecutor();
			executor.execute(cmdLine);
		}

	}

	public void clearDataBeforeRun(boolean clear) {
		this.clearDataBeforeRun = clear;
	}

	public void clearDataAfterRun(boolean clear) {
		this.clearDataAfterRun = clear;
	}

	@Override
	public String getDescription() {
		return testDescription;
	}

	protected Logger getLogger() {
		return LoggerFactory.getLogger(this.getClass().getName() + "."
				+ getDescription());
	}
}
