package sk.eea.triplestore.bench.stores;

import org.apache.marmotta.kiwi.config.KiWiConfiguration;
import org.apache.marmotta.kiwi.config.RegistryStrategy;
import org.apache.marmotta.kiwi.persistence.KiWiDialect;
import org.apache.marmotta.kiwi.persistence.pgsql.PostgreSQLDialect;
import org.apache.marmotta.kiwi.sail.KiWiStore;
import org.openrdf.repository.sail.SailRepository;

import sk.eea.triplestore.bench.Settings;

public class Kiwi extends AbstractSailStore {

	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		String defaultContext = "http://localhost/context/default";
		String inferredContext = "http://localhost/context/inferred";
		KiWiDialect dialect = new PostgreSQLDialect();
		KiWiConfiguration kiWiConfiguration = new KiWiConfiguration("test",
				repositoryUrl, "triplestore", "triplestore", dialect, defaultContext,
				inferredContext);
//		kiWiConfiguration.setRegistryStrategy(RegistryStrategy.DATABASE);
		KiWiStore store = new KiWiStore(kiWiConfiguration);
//		store.setDropTablesOnShutdown(true);
//		store.initialize();
//		store.shutDown();
		
		repository = new SailRepository(store);
		repository.initialize();
	}

}
