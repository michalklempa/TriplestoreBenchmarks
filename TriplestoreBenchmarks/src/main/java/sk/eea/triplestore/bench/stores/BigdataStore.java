package sk.eea.triplestore.bench.stores;


import sk.eea.triplestore.bench.Settings;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class BigdataStore extends AbstractSailStore {

	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		BigdataSail sail = new BigdataSail();
		repository = new BigdataSailRepository(sail);
		repository.initialize();
	}
}
