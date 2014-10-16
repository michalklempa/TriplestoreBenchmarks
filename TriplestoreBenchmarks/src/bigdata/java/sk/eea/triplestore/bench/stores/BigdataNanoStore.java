package sk.eea.triplestore.bench.stores;

import java.io.File;

import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import sk.eea.triplestore.bench.Settings;

public class BigdataNanoStore extends AbstractSailStore{
	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		repository = new com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository(repositoryUrl);
		if (repository == null) {
			throw new Exception("No repository " + repositoryId + " exists");
		}
		repository.initialize();
	}
	
	protected void clearDataBeforeRun(RepositoryConnection connection,
			Resource... resource) throws Exception {
		if (clearDataBeforeRun) {
			connection.clear(resource);
//			connection.commit();
		}
	}

	protected void clearDataAfterRun(RepositoryConnection connection,
			Resource... resource) throws Exception {
		if (clearDataAfterRun) {
			connection.clear(resource);
//			connection.commit();
		}
	}	
	
	public long[] testLoadData(String fileName, String uri) throws Exception {
		File inputFile = new File(fileName);
		getLogger().info(
				"########## test loadRDF, file: " + inputFile.getName()
						+ " ##########");

		long time = 0L;
		long count = 0L;

		Resource context = new URIImpl(uri); //nejde pre bigdata, musi byt null

		RepositoryConnection conn1 = repository.getConnection();
//		conn1.setAutoCommit(false);
		clearDataBeforeRun(conn1, context);
		conn1.close();

		RepositoryConnection conn = repository.getConnection();
		// Pre bigdata nano spadne
//		conn.setAutoCommit(false);
		try {
			long start = System.currentTimeMillis();
			conn.add(inputFile, uri, RDFFormat.RDFXML, context);
//			conn.commit();
			time = System.currentTimeMillis() - start;
			count = 0L;
			count = conn.size(context); // pre owlim spadne
			getLogger().info(
					"done, RDF count: " + count + ", time: " + time + " ms");
			clearDataAfterRun(conn, context);
		} finally {
			conn.close();
		}

		return new long[] { time, count };
	}
	
	@Override
	public long[] testLoadDataBatch(String fileName, String uri, int batchSize)
			throws Exception {

		return new long[] { 0L, 0L};
	}	
}
