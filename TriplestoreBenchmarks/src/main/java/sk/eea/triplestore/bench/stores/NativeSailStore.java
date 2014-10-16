package sk.eea.triplestore.bench.stores;

import java.io.File;

import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;

import sk.eea.triplestore.bench.Settings;

public class NativeSailStore extends AbstractSailStore {

	@Override
	public void initialize(Settings settings, String testId) throws Exception {
		super.initialize(settings, testId);
		
		repository = new SailRepository(new NativeStore(new File(repositoryUrl))); 
		if (repository == null) {
			throw new Exception("No repository " + repositoryId + " exists");
		}
		repository.initialize();
	}

}
