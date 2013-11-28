package sk.eea.triplestore.bench.stores;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public class InsertHandler implements RDFHandler {

	private RepositoryConnection conn;
	private RDFInserter inserter;
	private long commitSize;
	private long count;
	private List<Statement> statements = new ArrayList<Statement>();
	private URIImpl uri;


	/**
	 * 
	 * @param conn
	 * @param commitSize
	 */
	public InsertHandler(RepositoryConnection conn, long commitSize, String uri) {
		this.count = 0;
		this.inserter = new RDFInserter(conn);
		this.conn = conn;
		this.commitSize = commitSize;
//		this.uri = new URIImpl(uri);	//pre bigdata nejde, treba null
		this.uri = null;
	}

	/**
	 * 
	 */
	public void startRDF() throws RDFHandlerException {
		inserter.startRDF();
	}

	/**
	 * 
	 */
	public void endRDF() throws RDFHandlerException {
		try {
			if (statements.size() != 0) {
				conn.add(statements, uri);
			}
			conn.commit();
		} catch (RepositoryException e) {
			throw new RDFHandlerException(e);
		}
		inserter.endRDF();
	}

	/**
	 * 
	 */
	public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
		inserter.handleNamespace(prefix, uri);
	}

	/**
	 * 
	 */
	public void handleStatement(Statement st) throws RDFHandlerException {
		statements.add(st);
		count++;
		if (count % commitSize == 0) {
			try {
				conn.add(statements, uri);
				conn.commit();
				statements = new ArrayList<Statement>();
			} catch (RepositoryException e) {
				throw new RDFHandlerException(e);
			}
		}
	}

	/**
	 * 
	 */
	public void handleComment(String comment) throws RDFHandlerException {
		inserter.handleComment(comment);
	}
	
	public long getStatementsCount() {
		return count;
	}
}