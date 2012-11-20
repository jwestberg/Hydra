package com.findwise.hydra.mongodb;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.TailableIterator;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.Document.Status;
import com.findwise.hydra.common.DocumentFile;
import com.findwise.hydra.common.SerializationUtils;
import com.google.inject.Guice;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class MongoDocumentIOTest {
	private MongoConnector mdc;
	
	private MongoDocument test;
	private MongoDocument test2;
	private MongoDocument random;
	private File f;
	
	private Random r = new Random(System.currentTimeMillis());

	private void createAndConnect() throws Exception {
	
		mdc = Guice.createInjector(new TestModule("junit-MongoDocumentIO")).getInstance(MongoConnector.class);
		
		mdc.waitForWrites(true);
		mdc.connect();
	}
	
	@Before
	public void setUp() throws Exception {
		createAndConnect();
		mdc.getDB().getCollection(MongoDocumentIO.OLD_DOCUMENT_COLLECTION).drop();
		

		test = new MongoDocument();
		test.putContentField("name", "test");
		test.putContentField("number", 1);
		test.putMetadataField("date", new Date());
		
		test2 = new MongoDocument();
		test2.putContentField("name", "test");
		test2.putContentField("number", 2);
		test2.putMetadataField("date", new Date());
		
		f = new File("test1234.txt");
		if(f.exists()) {
			fail("Failing setup");
		}
		
		FileWriter fw = new FileWriter(f);
		
		fw.write("This is a file @"+System.currentTimeMillis());
		fw.close();
		
		random = new MongoDocument();
		
		mdc.getDocumentWriter().deleteAll();
	}
	
	private void insertTestDocs() {
		try {
			mdc.getDocumentWriter().insert(test);
			mdc.getDocumentWriter().insert(test2);
			mdc.getDocumentWriter().insert(random);
			
			DocumentFile df = new DocumentFile(test.getID(), f.getName(), new FileInputStream(f), "setup");
			mdc.getDocumentWriter().write(df);	
		} catch(Exception e) {
			throw new RuntimeException("Failed during insert of test documents", e);
		}
	}
	
	@AfterClass
	@BeforeClass
	public static void tearDown() throws Exception {
		new Mongo().getDB("junit-MongoDocumentIO").dropDatabase();
	}
	
	@After
	public void after() throws Exception {
		boolean successfulDelete = f.delete();
		for (int maxTries = 10; !successfulDelete && maxTries > 0; maxTries--) {
			System.gc();
			Thread.sleep(300);
			successfulDelete = f.delete();
		}
		if (!successfulDelete) {
			fail("TearDown failed to delete: " + f.getAbsolutePath());
		}

	}

	@Test
	public void testInsertDocument() {
		MongoDocument md = new MongoDocument();
		md.putContentField("name", "blahonga");
		
		MongoQuery mdq = new MongoQuery();
		mdq.requireContentFieldEquals("name", "blahonga");
		
		if(mdc.getDocumentReader().getDocument(mdq)!=null) {
			fail("Document already exists in database");
		}
		
		mdc.getDocumentWriter().insert(md);
		
		Document d = mdc.getDocumentReader().getDocument(mdq);
		if(d==null) {
			fail("No document in test database");
		}
	}

	@Test
	public void testUpdateDocument() {
		insertTestDocs();
		String field = "xyz";
		String content = "zyx";
		MongoQuery mdq = new MongoQuery();
		mdq.requireContentFieldEquals("name", "test");
		DatabaseDocument<MongoType> d = mdc.getDocumentReader().getDocument(mdq);
		d.putContentField(field, content);
		mdc.getDocumentWriter().update(d);
		mdq = new MongoQuery();
		mdq.requireContentFieldEquals(field, content);
		d = mdc.getDocumentReader().getDocument(mdq);
		
		if(!d.getContentField("xyz").equals("zyx")) {
			fail("Wrong data in updated field");
		}
	}

	@Test
	public void testGetDocuments() {
		insertTestDocs();
		MongoQuery mdq = new MongoQuery();
		List<DatabaseDocument<MongoType>> list = mdc.getDocumentReader().getDocuments(mdq, 3);
		if(list.size()!=3) {
			fail("Did not return all documents. Expected 3, found "+list.size());
		}
		boolean test1 = false, test2=false, random=false;
		for(Document d : list) {
			if("test".equals(d.getContentField("name"))) {
				if(((Integer)1).equals(d.getContentField("number")))
				{
					test1 = true;
				}
				else if(((Integer)2).equals(d.getContentField("number"))) {
					test2 = true;
				}
			}
			else {
				random = true;
			}
		}
		if (!test1 || !test2 || !random) {
			fail("Not all three documents were found");
		}
		
		mdq.requireContentFieldEquals("name", "test");
		list = mdc.getDocumentReader().getDocuments(mdq, 3);
		if(list.size()!=2) {
			fail("Wrong number of documents returned. Expected 2, found "+list.size());
		}
		
		mdq.requireContentFieldEquals("number", 2);
		list = mdc.getDocumentReader().getDocuments(mdq, 1);
		if(list.size()!=1) {
			fail("Wrong number of documents returned. Expected 1, found "+list.size());
		}
	}

	@Test
	public void testWriteDocumentFile() throws IOException{
		if(mdc.getDocumentReader().getDocumentFileNames(test2).size()!=0) {
			fail("Document already had files");
		}
		
		DocumentFile df = new DocumentFile(test2.getID(), f.getName(), new FileInputStream(f), "stage");

		mdc.getDocumentWriter().write(df);
		DocumentFile df2 = mdc.getDocumentReader().getDocumentFile(test2, f.getName());

		
		BufferedReader fr = new BufferedReader(new FileReader(f));
		BufferedReader fxr = new BufferedReader(new InputStreamReader(df2.getStream()));
		try {
			if (!fr.readLine().equals(fxr.readLine())) {
				fail("Content mismatch between saved file and loaded file");
			}
		}
		finally {
			fr.close();
			fxr.close();
		}
	}

	@Test
	public void testGetDocumentFile() throws IOException {
		insertTestDocs();
		
		DocumentFile fx = mdc.getDocumentReader().getDocumentFile(test, f.getName());

		if(!f.getName().equals(fx.getFileName())) {
			fail("Couldn't find document file");
		}

		BufferedReader fr = new BufferedReader(new FileReader(f));
		BufferedReader fxr = new BufferedReader(new InputStreamReader(fx.getStream()));
		try {
			if (!fr.readLine().equals(fxr.readLine())) {
				fail("Content mismatch between saved file and written file");
			}
		}
		finally {
			fr.close();
			fxr.close();
		}
		
	}
	
	@Test
	public void testGetDocumentDatabaseQuery() {
		insertTestDocs();
		
		DatabaseQuery<MongoType> dbq = new MongoQuery();
		dbq.requireContentFieldEquals("name", "test");
		dbq.requireContentFieldEquals("number", 2);
		Document d = mdc.getDocumentReader().getDocument(dbq);
		
		if(d.isEqual(test) || !d.isEqual(test2)) {
			fail("Incorrect document returned");
		}
		
	}

	@Test
	public void testGetAndTagDocumentDatabaseQueryString() {
		insertTestDocs();
		
		MongoQuery mdq = new MongoQuery();
		mdq.requireContentFieldExists("name");
		Document d = mdc.getDocumentWriter().getAndTag(mdq, "tag");
		if(d==null) {
			fail("Get and tag could not find any document");
		}
		if(d.getID()==null) {
			fail("Get and tag didn't get a document with a set ID");
		}
		if(d.getContentFields().size()==0) {
			fail("Get and tag didn't get any contents in the document");
		}
		if(d.getMetadataMap().size()==0) {
			fail("Get and tag didn't get any metadata in the document");
		}
		mdc.getDocumentWriter().getAndTag(new MongoQuery(), "tag");
		mdc.getDocumentWriter().getAndTag(new MongoQuery(), "tag");
		
		d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "tag");
		if(d!=null) {
			fail("Expected all documents to be tagged, but found "+d);
		}
	}

	@Test
	public void testDelete() {
		insertTestDocs();
		
		MongoQuery query = new MongoQuery();
		query.requireContentFieldNotEquals("name", "test");
		MongoDocument d = (MongoDocument) mdc.getDocumentReader().getDocument(query);
		Object id = d.getID();
		mdc.getDocumentWriter().delete(d);
		query.requireID(id);
		if(mdc.getDocumentReader().getDocument(query)!=null) {
			fail("Document failed to be deleted.");
		}
	}
	
	@Test
	public void testDiscardDocument() {		
		insertTestDocs();
		DatabaseDocument<MongoType> discarded_d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "DiscardedTag");
		
		DatabaseDocument<MongoType> d;
		List<Object> allDocs = new ArrayList<Object>();
		while ((d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "testRet")) != null) {
			allDocs.add(d.getID());
		}
		if (allDocs.contains(discarded_d.getID()) == false) {
			fail("Discarded document should still be there since it's not yet discarded");
		}
		
		mdc.getDocumentWriter().markDiscarded(discarded_d, "test_stage");
		
		while ((d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "testGetNonDiscarded")) != null) {
			if(discarded_d.getID() == d.getID()) {
				fail("Discarded document was retrieved after discard");
			}
		}
		
		DatabaseDocument<MongoType> old = mdc.getDocumentReader().getDocumentById(discarded_d.getID(), true);
		if(old==null) {
			fail("Failed to find the document in the 'old' database");
		}
		
		if(old.getStatus()!=Status.DISCARDED) {
			fail("No discarded flag on the document");
		}
	}
	
	@Test
	public void testFailedDocument() {	
		insertTestDocs();
		DatabaseDocument<MongoType> failed = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "failedTag");
		
		mdc.getDocumentWriter().markFailed(failed, "test_stage");
		
		if(mdc.getDocumentReader().getDocumentById(failed.getID())!=null) {
			fail("Failed document was retrieved after discard");
		}
		
		DatabaseDocument<MongoType> old = mdc.getDocumentReader().getDocumentById(failed.getID(), true);
		if(old==null) {
			fail("Failed to find the document in the 'old' database");
		}
		
		if(old.getStatus()!=Status.FAILED) {
			fail("No failed flag on the document");
		}
	}
	
	@Test
	public void testPendingDocument() {
		insertTestDocs();
		DatabaseDocument<MongoType> pending = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "pending");
		
		mdc.getDocumentWriter().markPending(pending, "test_stage");
		
		pending = mdc.getDocumentWriter().getDocumentById(pending.getID());
		
		if(pending.getStatus()!=Status.PENDING) {
			fail("No PENDING flag on the document");
		}
	}
	
	@Test
	public void testProcessedDocument() {
		insertTestDocs();	
		DatabaseDocument<MongoType> processed = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "processed");
		
		mdc.getDocumentWriter().markProcessed(processed, "test_stage");
		
		processed = mdc.getDocumentWriter().getDocumentById(processed.getID(), true);
		
		if(processed.getStatus()!=Status.PROCESSED) {
			fail("No PROCESSED flag on the document");
		}
	}
	
	@Test
	public void testActiveDatabaseSize() {
		insertTestDocs();
		if(mdc.getDocumentReader().getActiveDatabaseSize() != 3) {
			fail("Not the correct active database size. Expected 3 got: "+mdc.getDocumentReader().getActiveDatabaseSize());
		}
		List<DatabaseDocument<MongoType>> list = mdc.getDocumentReader().getDocuments(new MongoQuery(), 2);
		mdc.getDocumentWriter().markProcessed(list.get(0), "x");
		mdc.getDocumentWriter().markDiscarded(list.get(1), "x");
		if(mdc.getDocumentReader().getActiveDatabaseSize() != 1) {
			fail("Not the correct active database size after processed and discard, expected 1 found " + mdc.getDocumentReader().getActiveDatabaseSize());
		}
	}
	
	
	@Test
	public void testPrepare() {
		DB db = mdc.getDB();
		
		if(db.getCollectionNames().contains(MongoDocumentIO.OLD_DOCUMENT_COLLECTION)) {
			fail("Collection already exists");
		}
		mdc.getDocumentWriter().prepare();
		
		if(!db.getCollectionNames().contains(MongoDocumentIO.OLD_DOCUMENT_COLLECTION)) {
			fail("Collection was not created");
		}
		
		if(!isCapped()) {
			fail("Collection not capped");
		}
	}
	
	private boolean isCapped() {
		return mdc.getDB().getCollection(MongoDocumentIO.OLD_DOCUMENT_COLLECTION).isCapped();
	}
	
	@Test
	public void testConnectPrepare() throws Exception {
		mdc.getDB().dropDatabase();
		while(mdc.getDB().getCollection(MongoStatusIO.HYDRA_COLLECTION_NAME).count()!=0) {
			mdc.getDB().getCollection(MongoStatusIO.HYDRA_COLLECTION_NAME).remove(new BasicDBObject(), WriteConcern.SAFE);
			Thread.sleep(50);
		}
		
		if(mdc.getStatusReader().hasStatus()) {
			fail("Test error");
		}
		
		Assert.assertFalse(isCapped());
		
		mdc.connect();

		if(!isCapped()) {
			fail("Collection was not capped on connect");
		}
	}
	
	@Test
	public void testRollover() throws Exception {
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();

		for(int i=0; i<mdc.getStatusReader().getStatus().getNumberToKeep(); i++) {
			dw.insert(new MongoDocument());
			DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
			dw.markProcessed(dd, "tag");
		}
		
		if(mdc.getDocumentReader().getActiveDatabaseSize()!=0) {
			fail("Still some active docs..");
		}
		
		if(mdc.getDocumentReader().getInactiveDatabaseSize()!=mdc.getStatusReader().getStatus().getNumberToKeep()) {
			fail("Incorrect number of old documents kept");
		}
		
		dw.insert(new MongoDocument());
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		if(mdc.getDocumentReader().getActiveDatabaseSize()!=0) {
			fail("Still some active docs..");
		}
		if(mdc.getDocumentReader().getInactiveDatabaseSize()!=mdc.getStatusReader().getStatus().getNumberToKeep()) {
			fail("Incorrect number of old documents kept: "+ mdc.getDocumentReader().getInactiveDatabaseSize());
		}
	}
	
	@Test
	public void testNullFields() throws Exception {
		MongoDocumentIO dw = (MongoDocumentIO) mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();
		md.putContentField("field", "value");
		md.putContentField("nullfield", null);
		dw.insert(md);
		
		MongoDocument indb = dw.getAndTag(new MongoQuery(), "tag");
		
		if(indb.hasContentField("nullfield")) {
			fail("Null field was persisted in database on insert");
		}
		Assert.assertEquals("value", indb.getContentField("field"));
		
		md.putContentField("field", null);
		
		dw.update(md);

		indb = dw.getAndTag(new MongoQuery(), "tag2");

		if(indb.hasContentField("field")) {
			fail("Null field was persisted in database on update");
		}
		
	}
	
	@Test
	public void testIdSerialization() throws Exception {
		ObjectId id = new ObjectId();
		
		String serialized = SerializationUtils.toJson(id);
		Object deserialized = mdc.getDocumentReader().toDocumentIdFromJson(serialized);
		if(!id.equals(deserialized)) {
			fail("Serialization failed from json string");
		}
		deserialized = mdc.getDocumentReader().toDocumentId(SerializationUtils.toObject(serialized));
		if(!id.equals(deserialized)) {
			fail("Serialization failed from primitive");
		}
	}
	
	@Test
	public void testInactiveIterator() throws Exception {
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		
		TailableIterator<MongoType> it = mdc.getDocumentReader().getInactiveIterator();
		
		TailReader tr = new TailReader(it);
		tr.start();
		
		MongoDocument first = new MongoDocument();
		first.putContentField("num", 1);
		dw.insert(first);
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		while(tr.lastRead>System.currentTimeMillis() && tr.isAlive()) {
			Thread.sleep(50);
		}
		
		if(!tr.isAlive()) {
			fail("TailableReader died");
		}
		
		long lastRead = tr.lastRead;
		
		if(!tr.lastReadDoc.getContentField("num").equals(1)) {
			fail("Last doc read was not the correct document!");
		}
		
		MongoDocument second = new MongoDocument();
		second.putContentField("num", 2);
		dw.insert(second);
		dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		while(tr.lastRead==lastRead) {
			Thread.sleep(50);
		}

		if (!tr.lastReadDoc.getContentField("num").equals(2)) {
			fail("Last doc read was not the correct document!");
		}

		
		if(tr.hasError) {
			fail("An exception was thrown by the TailableIterator prior to interrupt");
		}
		
		tr.interrupt();

		long interrupt = System.currentTimeMillis();
		
		while (tr.isAlive() && (System.currentTimeMillis()-interrupt)<10000) {
			Thread.sleep(50);
		}
		
		if(tr.isAlive()) {
			fail("Unable to interrupt the tailableiterator");
		}
		
		if(tr.hasError) {
			fail("An exception was thrown by the TailableIterator after interrupt");
		}
	}
	
	@Test
	public void testDoneContentTransfer() throws Exception {
		mdc.getDocumentWriter().prepare();
		
		MongoDocument d = new MongoDocument();
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		
		mdc.getDocumentWriter().insert(d);

		d = mdc.getDocumentReader().getDocumentById(d.getID());
		
		d.putContentField(getRandomString(5), getRandomString(20));
		
		mdc.getDocumentWriter().update(d);
		
		mdc.getDocumentWriter().markProcessed(d, "x");
		
		MongoDocument d2 = mdc.getDocumentReader().getDocumentById(d.getID(), true);
		
		if(d.getContentFields().size()!=d2.getContentFields().size()) {
			fail("Processed document did not have the correct number of content fields");
		}
		
		for(String field : d.getContentFields()) {
			if(!d2.hasContentField(field)) {
				fail("Processed document did not have the correct content fields");
			}
			
			if(!d2.getContentField(field).equals(d.getContentField(field))) {
				fail("Processed document did not have the correct data in the content fields");
			}
		}
	}
	
	@Ignore
	@Test
	public void testInsertLargeDocument() throws Exception {
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		
		MongoDocument d = new MongoDocument();
		makeDocumentTooLarge(d);
		
		if(dw.insert(d)) {
			fail("No error inserting big document");
		}
	}
	
	@Test
	@Ignore
	public void testUpdateLargeDocument() throws Exception {

		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		
		
		
		MongoDocument d = new MongoDocument();
		d.putContentField("some_field", "some data");
		
		dw.insert(d);
		
		makeDocumentTooLarge(d);
		
		if(dw.update(d)) {
			fail("No error updating big document");
		}
	}
	
	private void makeDocumentTooLarge(MongoDocument d) {
		int maxMongoDBObjectSize = mdc.getDB().getMongo().getConnector().getMaxBsonObjectSize();
		while(d.toJson().getBytes().length <= maxMongoDBObjectSize) {
			d.putContentField(getRandomString(5), getRandomString(1000000));
		}
	}
	
	int testReadCount = 1;
	@Test
	public void testReadStatus() throws Exception {
		mdc.getDocumentWriter().prepare();
		
		testReadCount = (int)mdc.getStatusReader().getStatus().getNumberToKeep(); 
		
		TailReader tr = new TailReader(mdc.getDocumentReader().getInactiveIterator());
		tr.start();
		
		Thread t = new Thread() {
			public void run() {
				try {
					insertDocuments(testReadCount);
					processDocuments(testReadCount/3);
					failDocuments(testReadCount/3);
					discardDocuments(testReadCount - (testReadCount/3)*2);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
		};
		t.start();
		
		long timer = System.currentTimeMillis();
		
		while (tr.count < testReadCount && (System.currentTimeMillis()-timer)<10000) {
			Thread.sleep(50);
		}
		
		if(tr.count < testReadCount) {
			fail("Did not see all documents");
		}
		
		if(tr.count > testReadCount) {
			fail("Saw too many documents");
		}
		
		if(tr.countProcessed != testReadCount/3) {
			fail("Incorrect number of processed documents. Expected "+testReadCount/3+" but saw "+tr.countProcessed);
		}
		
		if(tr.countFailed != testReadCount/3) {
			fail("Incorrect number of failed documents. Expected "+testReadCount/3+" but saw "+tr.countFailed);
		}
		
		if(tr.countDiscarded != testReadCount - (testReadCount/3)*2) {
			fail("Incorrect number of discarded documents. Expected "+(testReadCount - (testReadCount/3)*2)+" but saw "+tr.countDiscarded);
		}
		
		tr.interrupt();
	}
	
	public long processDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markProcessed(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}
	
	public long failDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markFailed(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}
	
	public long discardDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markDiscarded(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}
	
	public long insertDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		for(int i=0; i<count; i++) {
			MongoDocument d = new MongoDocument();
			d.putContentField(getRandomString(5), getRandomString(20));
			mdc.getDocumentWriter().insert(d);
		}
		return System.currentTimeMillis()-start;
	}

	
	private String getRandomString(int length) {
		char[] ca = new char[length];

		for (int i = 0; i < length; i++) {
			ca[i] = (char) ('A' + r.nextInt(26));
		}

		return new String(ca);
	}
	

	public static class TailReader extends Thread {
		private TailableIterator<MongoType> it;
		public long lastRead = Long.MAX_VALUE;
		public DatabaseDocument<MongoType> lastReadDoc = null;
		boolean hasError = false;
		
		int countFailed = 0;
		int countProcessed = 0;
		int countDiscarded = 0;
		
		int count = 0;
		
		public TailReader(TailableIterator<MongoType> it) {
			this.it = it;
		}

		public void run() {
			try {
				while (it.hasNext()) {
					lastRead = System.currentTimeMillis();
					lastReadDoc = it.next();
					
					Status s = lastReadDoc.getStatus();
					
					if(s==Status.DISCARDED) {
						countDiscarded++;
					} else if (s == Status.PROCESSED) {
						countProcessed++;
					} else if (s == Status.FAILED) {
						countFailed++;
					}
					
					count++;
				}
			} catch (Exception e) {
				e.printStackTrace();
				hasError = true;
			}
		}

		public void interrupt() {
			it.interrupt();
		}
	}
}
