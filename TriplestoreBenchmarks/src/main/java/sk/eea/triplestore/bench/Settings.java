package sk.eea.triplestore.bench;

import java.util.Properties;

public class Settings {
	private Properties props;

	public String output;
	public int runs;
	public String repository_type;
	public String repository_url;
	public int file_count;
	public int sparql_count;

	
	public Settings() {
		props = new Properties();
	}
	
	public void loadSettings() throws Exception {
		props.load(getClass().getClassLoader().getResourceAsStream("test.properties"));
		runs  = Integer.parseInt(props.getProperty("test.runs.count"));
		output = props.getProperty("test.output.file");
		repository_type = props.getProperty("repository.type");
		repository_url = props.getProperty("repository.url");
		file_count = Integer.parseInt(props.getProperty("import.files.count"));
		sparql_count = Integer.parseInt(props.getProperty("sparql.query.count"));
	}

	public String getFileName(int fileIndex) {
		return props.getProperty("import.rdf.file." + fileIndex);
	}
	public String getFileUri(int fileIndex) {
		return props.getProperty("import.rdf.uri." + fileIndex);
	}
	public String getSparqlQuery(int index) {
		return props.getProperty("sparql.query." + index);
	}
	public String getProviderData(String key) {
		return props.getProperty(key);
	}
	
	
	
}
