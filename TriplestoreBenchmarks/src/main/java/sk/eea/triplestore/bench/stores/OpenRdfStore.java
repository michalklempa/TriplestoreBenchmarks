package sk.eea.triplestore.bench.stores;

import org.openrdf.repository.manager.RemoteRepositoryManager;
import org.openrdf.repository.manager.RepositoryManager;

import sk.eea.triplestore.bench.Settings;

public class OpenRdfStore extends AbstractSailStore {

    @Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		
		RepositoryManager repoManager = new RemoteRepositoryManager(repositoryUrl
				);
		repoManager.initialize();
		repository = repoManager.getRepository(repositoryId);
		if (repository == null) {
			throw new Exception("No repository " + repositoryId + " exists");
		}
		repository.initialize();
	}

}
