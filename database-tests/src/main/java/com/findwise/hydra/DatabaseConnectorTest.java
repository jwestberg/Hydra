package com.findwise.hydra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

public abstract class DatabaseConnectorTest<T extends DatabaseType> {
	
	protected abstract DatabaseConnector<T> getNewConnector();
	protected abstract DatabaseConnector<T> getNewConnector(DatabaseConfiguration conf);
	
	private static Random random = new Random();
	private DatabaseConnector<?> connectedDatabaseConnector;
	
	@SuppressWarnings("unchecked")
	private DatabaseConnector<T> getInstance() throws IOException {
		if(connectedDatabaseConnector == null) {
			connectedDatabaseConnector = getNewConnector();
			connectedDatabaseConnector.connect();
		}
		return (DatabaseConnector<T>) connectedDatabaseConnector;
	}
	
	@Test
	public void testConnect() throws IOException {
		DatabaseConnector<T> dbc = getNewConnector();
		dbc.connect();
		assertTrue(dbc.isConnected());
	}

	/**
	 * Untestable? What does this mean?
	 * 
	 * TODO: Refactor interface
	 */
	@Test
	@Ignore
	public void testWaitForWrites() {
		fail("Not yet implemented");
	}

	@Test
	@Ignore
	public void testIsWaitingForWrites() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetPipelineReader() throws IOException {
		assertNotNull(getInstance().getPipelineReader());
	}

	@Test
	public void testGetPipelineWriter() throws IOException {
		assertNotNull(getInstance().getPipelineWriter());
	}

	@Test
	public void testGetDocumentReader() throws IOException {
		assertNotNull(getInstance().getDocumentReader());
	}

	@Test
	public void testGetDocumentWriter() throws IOException {
		assertNotNull(getInstance().getDocumentWriter());
	}

	@Test
	public void testConvertQuery() throws IOException {
		LocalQuery lq = new LocalQuery();
		lq.requireAction(Action.values()[random.nextInt(Action.values().length)]);
		//assertEquals(lq.getAction(), getInstance().convert(lq).);
	}

	@Test
	public void testConvertDocument() throws IOException, ConversionException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("string", "value");
		ld.putContentField("int", 1);
		ld.putContentField("double", 1.1);
		ld.putContentField("list", Arrays.asList(new String[]{"one", "two", "three", "four", "five", "six"}));
		DatabaseDocument<T> dd = getInstance().convert(ld);
		assertTrue(dd.hasContentField("string"));
		assertTrue(dd.hasContentField("int"));
		assertTrue(dd.hasContentField("double"));
		assertTrue(dd.hasContentField("list"));
		assertEquals(ld.getContentField("string"), dd.getContentField("string"));
		assertEquals(ld.getContentField("int"), (Integer)dd.getContentField("int"));
		assertEquals(ld.getContentField("double"), dd.getContentField("double"));
		assertEquals(((List<?>)ld.getContentField("list")).size(), ((List<?>)dd.getContentField("list")).size());
		for(Object o : (List<?>) ld.getContentField("list")) {
			assertTrue(((List<?>)dd.getContentField("list")).contains(o));
		}
	}

	@Test
	public void testGetStatusWriter() throws IOException {
		assertNotNull(getInstance().getStatusWriter());
	}

	@Test
	public void testGetStatusReader() throws IOException {
		assertNotNull(getInstance().getStatusReader());
	}

}
