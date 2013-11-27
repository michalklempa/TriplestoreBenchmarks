package sk.eea.triplestore.bench.stores;

import org.openrdf.repository.manager.RemoteRepositoryManager;
import org.openrdf.repository.manager.RepositoryInfo;
import org.openrdf.repository.manager.RepositoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sk.eea.triplestore.bench.Settings;

public class OwlimStore extends AbstractSailStore {
	private static final Logger logger = LoggerFactory.getLogger(OwlimStore.class);

	public void initialize(Settings settings) throws Exception  {
		String repositoryId = settings.getProviderData("owlim.repository.id");
		RepositoryManager repoManager = new RemoteRepositoryManager(settings.repository_url);
		repoManager.initialize();
		RepositoryInfo info  = repoManager.getRepositoryInfo(repositoryId);
		if (info == null) {;
			throw new Exception("Repository " + repositoryId + " does not exists");
		}
		repository = repoManager.getRepository(repositoryId);
		repository.initialize();
		
	}

	public String getDescription() {
		return "Owlim";
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
