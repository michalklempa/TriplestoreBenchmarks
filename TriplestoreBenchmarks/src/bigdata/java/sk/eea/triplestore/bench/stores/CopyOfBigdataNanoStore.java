package sk.eea.triplestore.bench.stores;

import java.io.File;

import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import sk.eea.triplestore.bench.Settings;

public class CopyOfBigdataNanoStore extends AbstractSailStore{
	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		//new com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager();
		repository = new com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository(repositoryUrl);
//		RepositoryManager repoManager = new RemoteRepositoryManager(repositoryUrl				);
//		repoManager.initialize();
//		repository = repoManager.getRepository(repositoryId);
		if (repository == null) {
			throw new Exception("No repository " + repositoryId + " exists");
		}
		repository.initialize();
	}
	
	@Override
	public long[] testLoadData(String fileName, String uri) throws Exception {
		File inputFile = new File(fileName);
		getLogger().info(
				"########## test loadRDF, file: " + inputFile.getName()
						+ " ##########");

		long time = 0L;
		long count = 0L;

		Resource context = new URIImpl(uri); //nejde pre bigdata, musi byt null
//		Resource context = null;

		RepositoryConnection conn1 = repository.getConnection();
		// Pre bigdata nano spadne
		//conn.setAutoCommit(false);
		clearDataBeforeRun(conn1, context);
		conn1.close();
		
		RepositoryConnection conn = repository.getConnection();
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
}
