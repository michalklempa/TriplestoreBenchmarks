package sk.eea.triplestore.bench.stores;

import org.openrdf.repository.manager.RemoteRepositoryManager;
import org.openrdf.repository.manager.RepositoryInfo;
import org.openrdf.repository.manager.RepositoryManager;

import sk.eea.triplestore.bench.Settings;

public class VirtuosoSailStore extends AbstractSailStore {

    @Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		RepositoryManager repoManager = new RemoteRepositoryManager(repositoryUrl);
		repoManager.initialize();
		RepositoryInfo info  = repoManager.getRepositoryInfo(repositoryId);
		if (info == null) {;
			throw new Exception("Repository " + repositoryId + " does not exists");
		}
		repository = repoManager.getRepository(repositoryId);
		repository.initialize();
	}
}
