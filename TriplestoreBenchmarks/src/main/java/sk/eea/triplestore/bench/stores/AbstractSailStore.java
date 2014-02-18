package sk.eea.triplestore.bench.stores;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
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
	public void initialize(Settings settings, String testId)
			throws Exception {
		this.testId = testId;
		outputFile = settings.getValue("test." + testId + ".output.file");
		repositoryType = settings.getValue("test." + testId
				+ ".repository.type");
		repositoryId = settings.getValue("test." + testId
				+ ".repository.id");
		repositoryUrl = settings.getValue("test." + testId
				+ ".repository.url");

		startupCommand = settings.getValue("test." + testId
				+ ".startup.command");
		shutdownCommand = settings.getValue("test." + testId
				+ ".shutdown.command");

		testDescription = settings.getValue("test." + testId
				+ ".description");
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
			connection.commit();
		}
	}

	protected void clearDataAfterRun(RepositoryConnection connection,
			Resource... resource) throws Exception {
		if (clearDataAfterRun) {
			connection.clear(resource);
			connection.commit();
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

		Resource context = new URIImpl(uri); //nejde pre bigdata, musi byt null
//		Resource context = null;

		RepositoryConnection conn = repository.getConnection();
		// Pre bigdata nano spadne
		conn.setAutoCommit(false);
		clearDataBeforeRun(conn, context);
		try {
			long start = System.currentTimeMillis();
			conn.add(inputFile, uri, RDFFormat.RDFXML, context);
			conn.commit();
			time = System.currentTimeMillis() - start;
			count = 0L;
			conn.size(context); // pre owlim spadne
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
		RepositoryConnection conn = repository.getConnection();
		long time = 0L;
		long count = 0L;

		conn.setAutoCommit(false);
		clearDataBeforeRun(conn, context);
		RDFParser parser = Rio.createParser(RDFFormat.RDFXML);
		InsertHandler handler = new InsertHandler(conn, batchSize, uri);
		parser.setRDFHandler(handler);
		try {
			long start = System.currentTimeMillis();
			parser.parse(new FileInputStream(inputFile), uri);
			conn.commit();
			time = System.currentTimeMillis() - start;
			count = handler.getStatementsCount();
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
