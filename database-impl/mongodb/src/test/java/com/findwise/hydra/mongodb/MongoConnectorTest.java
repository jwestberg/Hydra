package com.findwise.hydra.mongodb;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.local.LocalDocument;
import com.google.inject.Guice;
import com.mongodb.Mongo;

public class MongoConnectorTest {
	MongoConnector mdc;
	
	@Before
	public void setUp() throws Exception {
		mdc = Guice.createInjector(new TestModule("junit-MongoConnectorTest")).getInstance(MongoConnector.class);
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		new Mongo().getDB("junit-MongoConnectorTest").dropDatabase();
	}
	
	@Test(expected = ConversionException.class)
	public void testConversionWithNullCharacterInFieldName() throws ConversionException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("field\0000name", "field value");
		mdc.convert(ld);
	}
	
	@Test(expected = ConversionException.class)
	public void testConversionWithNullCharacterInList() throws ConversionException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("fieldname", Arrays.asList(new String[] {"some", "string", "with", "null\u0000here"}));
		mdc.convert(ld);
	}

	@Test(expected = ConversionException.class)
	public void testConversionWithNullCharacterInString() throws ConversionException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("fieldname", "some\u0000null");
		mdc.convert(ld);
	}

	@Test(expected = ConversionException.class)
	public void testConversionWithNullCharacterInMap() throws ConversionException {
		LocalDocument ld = new LocalDocument();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("NUL \u0000 here", "");
		ld.putContentField("field", map);
		try {
			mdc.convert(ld);
		} catch (ConversionException e) {
			map.clear();
			map.put("nulvalue", "the \u0000 value");
			ld.putContentField("field", map);
			mdc.convert(ld);
		}
		fail("Was able to put in a map with a null in a field name");
	}
}
