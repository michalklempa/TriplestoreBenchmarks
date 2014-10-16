package sk.eea.triplestore.bench.stores;

import sk.eea.triplestore.bench.Settings;
import virtuoso.sesame2.driver.VirtuosoRepository;

public class VirtuosoStore extends AbstractSailStore {




    @Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		repository = new VirtuosoRepository(repositoryUrl, 
				"dba", 
				"dba", true);
		repository.initialize();
	}
}
