package sk.eea.triplestore.bench.stores;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.Settings;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class BigdataStore extends AbstractSailStore {
	private static final Logger logger = LoggerFactory.getLogger(BigdataStore.class);

	public void initialize(Settings settings) throws Exception {
		BigdataSail sail = new BigdataSail();
		repository = new BigdataSailRepository(sail);
		repository.initialize();
	}

	public String getDescription() {
		return "BigData";
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	
}
