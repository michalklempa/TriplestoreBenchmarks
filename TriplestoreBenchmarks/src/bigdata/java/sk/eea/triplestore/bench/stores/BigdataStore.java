package sk.eea.triplestore.bench.stores;

import java.io.File;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.helpers.ParseErrorLogger;

import sk.eea.triplestore.bench.Settings;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class BigdataStore extends AbstractSailStore {

	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		BigdataSail sail;
		if (repositoryId != null) {
			// use one of our pre-configured option-sets or "modes"
			Properties properties = new Properties();
			properties.load(getClass().getClassLoader().getResourceAsStream(repositoryId));
	
			// create a backing file for the database
			properties.setProperty(
			    BigdataSail.Options.FILE,
			    repositoryUrl
			    );
	
			// instantiate a sail and a Sesame repository
			sail = new BigdataSail(properties);
		} else {
			sail = new BigdataSail();
		}
		
		repository = new BigdataSailRepository(sail);
		repository.initialize();
		Logger.getLogger(ParseErrorLogger.class).setLevel(Level.ERROR);
	}
	/*
	public long[] testLoadData(String fileName, String uri) throws Exception {
		File inputFile = new File(fileName);
		getLogger().info(
				"########## test loadRDF, file: " + inputFile.getName()
						+ " ##########");

		long time = 0L;
		long count = 0L;

		Resource context = new URIImpl(uri); //nejde pre bigdata, musi byt null

		RepositoryConnection conn1 = repository.getConnection();
		conn1.setAutoCommit(false);
		clearDataBeforeRun(conn1, context);
		conn1.close();

		RepositoryConnection conn = repository.getConnection();
		conn.setAutoCommit(false);
		try {
			long start = System.currentTimeMillis();
			conn.add(inputFile, uri, RDFFormat.RDFXML, context);
			conn.commit();
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
	}	*/
}
