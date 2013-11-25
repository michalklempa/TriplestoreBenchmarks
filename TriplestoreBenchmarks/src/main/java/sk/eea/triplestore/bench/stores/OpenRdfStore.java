package sk.eea.triplestore.bench.stores;

import org.openrdf.repository.config.RepositoryConfig;
import org.openrdf.repository.manager.RemoteRepositoryManager;
import org.openrdf.repository.manager.RepositoryManager;
import org.openrdf.repository.sail.config.SailRepositoryConfig;
import org.openrdf.sail.inferencer.fc.config.ForwardChainingRDFSInferencerConfig;
import org.openrdf.sail.nativerdf.config.NativeStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.Settings;

public class OpenRdfStore extends AbstractSailStore {
	private static final Logger logger = LoggerFactory.getLogger(OpenRdfStore.class);

	public void initialize(Settings settings) throws Exception  {
		String repositoryId = settings.getProviderData("openrdf.repository.id");
		RepositoryManager repoManager = new RemoteRepositoryManager(settings.repository_url);
		repoManager.initialize();
		ForwardChainingRDFSInferencerConfig inferStoreConfig = new ForwardChainingRDFSInferencerConfig(new NativeStoreConfig());
		RepositoryConfig cfg = new RepositoryConfig(repositoryId, new SailRepositoryConfig(inferStoreConfig));
		repoManager.addRepositoryConfig(cfg);
		repository = repoManager.getRepository(repositoryId);
		repository.initialize();
		
	}

	public String getDescription() {
		return "OpeRDF/Sesame";
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
