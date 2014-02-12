package sk.eea.triplestore.bench;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Settings {
	private Properties properties;


	public Settings() throws FileNotFoundException, IOException {
		properties = new Properties();
		String propertiesLocation = System.getProperty("test.properties.location");
		if (propertiesLocation != null && !propertiesLocation.isEmpty()) {
			properties.load(new FileInputStream(propertiesLocation));
		} else {
			properties.load(getClass().getClassLoader().getResourceAsStream(
					"test.properties"));
		}
	}

	public String getValue(String key) {
		String systemValue = System.getProperty(key);
		if (systemValue != null) {
			return systemValue;
		}
		return properties.getProperty(key);
	}

	public String getFileName(int fileIndex) {
		return getValue("import.rdf.file." + fileIndex);
	}

	public String getFileUri(int fileIndex) {
		return getValue("import.rdf.uri." + fileIndex);
	}

	public String getSparqlQuery(int index) {
		return getValue("sparql.query." + index);
	}
}
