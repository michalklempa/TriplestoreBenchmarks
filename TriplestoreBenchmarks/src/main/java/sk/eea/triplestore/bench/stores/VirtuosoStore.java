package sk.eea.triplestore.bench.stores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.Settings;
import virtuoso.sesame2.driver.VirtuosoRepository;

public class VirtuosoStore extends AbstractSailStore {

	private static final Logger logger = LoggerFactory.getLogger(VirtuosoStore.class);


	public String getDescription() {
		return "Virtuoso";
	}



	public void initialize(Settings settings) throws Exception {
		repository = new VirtuosoRepository(settings.repository_url, 
				settings.getProviderData("virtuoso.repository.user"), 
				settings.getProviderData("virtuoso.repository.password"), true);
		repository.initialize();
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
