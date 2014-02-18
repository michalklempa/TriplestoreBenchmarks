package sk.eea.triplestore.bench.stores;

import java.util.Properties;

import sk.eea.triplestore.bench.Settings;
import sk.eea.triplestore.bench.stores.AbstractSailStore;

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
	}
}
