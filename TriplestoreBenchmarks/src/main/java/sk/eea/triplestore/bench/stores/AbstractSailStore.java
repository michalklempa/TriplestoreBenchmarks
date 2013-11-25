package sk.eea.triplestore.bench.stores;

import java.io.File;
import java.io.FileInputStream;

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

public abstract class AbstractSailStore implements Store {
	
	protected Repository repository;
	
	/**
	 * 
	 */
	public long testLoadData(String fileName, String uri) throws Exception {
		File inputFile = new File(fileName);
		getLogger().info("########## test loadRDF, file: " + inputFile.getName() + " ##########" );
		long time = 0L;
		

		Resource context = new URIImpl(uri);
		RepositoryConnection conn = repository.getConnection();
		conn.setAutoCommit(false);
		conn.clear(context);
		try {
			long start = System.currentTimeMillis();
			conn.add(inputFile, uri, RDFFormat.RDFXML);
			time = System.currentTimeMillis() - start;
			getLogger().info("done, RDF count: " + conn.size(context) + ", time: " + time + " ms");
		} finally {
			conn.commit();
			conn.close();
		}	
		
		return time;
	}

	/**
	 * 
	 */
	public long testLoadDataBatch(String fileName, String uri, int batchSize) throws Exception {
		Resource context = new URIImpl(uri);
		
		File inputFile = new File(fileName);
		getLogger().info("########## test loadRDFInBatch, file: " + inputFile.getName() + ", batchSize: " + batchSize + "  ##############");
		RepositoryConnection conn = repository.getConnection();
		long time = 0L;
		conn.clear(context);
		conn.setAutoCommit(false);
		RDFParser parser = Rio.createParser(RDFFormat.RDFXML);
		parser.setRDFHandler(new BatchInsertHandler(conn, batchSize, uri));
		try {
			long start = System.currentTimeMillis();
			parser.parse(new FileInputStream(inputFile), uri);
			time =  System.currentTimeMillis() - start;
			getLogger().info("done, RDF count: " + conn.size(context) + ", time: " + time + " ms");
	
		} finally {
			conn.commit();
			conn.close();
		}
		
		return time;
	}

	/**
	 * 
	 */
	public long testSparql(String sparql) throws Exception {
		getLogger().info("########## test SPARQLQuery ##############");
		RepositoryConnection conn = repository.getConnection();
		long time = 0L;
		
		int responseCount = 0;
		try {
			long start = System.currentTimeMillis();
			TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
			TupleQueryResult result = tupleQuery.evaluate();
			try {
				while (result.hasNext()) {
					result.next();
					responseCount++;
				}
				time = System.currentTimeMillis() - start;
				getLogger().info("done, SPARQL returned: " + responseCount + ", time: " + time + " ms");
			} finally {
				result.close();
			}
		} finally {
			conn.close();
		}
		return time;
	}

	/**
	 * 
	 */
	public void shutDown() throws Exception {
		if (repository != null) {
			repository.shutDown();
		}
		
	}

	
	protected abstract Logger getLogger();
	

}
