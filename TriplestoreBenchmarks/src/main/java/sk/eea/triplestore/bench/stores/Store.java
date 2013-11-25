package sk.eea.triplestore.bench.stores;

import sk.eea.triplestore.bench.Settings;

public interface Store {

	public void initialize(Settings settings) throws Exception;
	public void shutDown() throws Exception;
	public String getDescription();

	public long testLoadData(String fileName, String uri) throws Exception;
	public long testLoadDataBatch(String fileName, String uri, int batchSize) throws Exception;
	public long testSparql(String sparql) throws Exception;
	
}
