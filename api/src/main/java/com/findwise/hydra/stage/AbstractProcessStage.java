package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.Map;

import org.apache.http.ParseException;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Class to use for creating new process steps in Hydra.
 * 
 * @author anton.hagerstrand
 * @author simon.stenstrom
 * @author anders.rask
 * @author joel.westberg
 * 
 */
public abstract class AbstractProcessStage extends AbstractStage {
    Logger logger = LoggerFactory.getLogger(AbstractProcessStage.class);

	@Parameter(description="If set, indicates that the document being processed should be FAILED if a ProcessException is thrown by the stage. If not set, the error will only be persisted and the document written back to Hydra.")
	private boolean failDocumentOnProcessException = false;
	
	public static final int NUM_RESERVED_ARGUMENTS = 3;
	private long holdInterval = DEFAULT_HOLD_INTERVAL;

	/**
	 * Fetches a document to be processed from the RemotePipeline
	 * 
	 * @return A document to be processed
	 * @throws ParseException
	 * @throws IOException
	 * @throws JsonException
	 */
	protected LocalDocument fetch() throws ParseException, IOException,
			JsonException {
		return getRemotePipeline().getDocument(getQuery());
	}

	/**
	 * Saves the modified document to the RemotePipeline.
	 * 
	 * @throws IOException
	 * @throws JsonException
	 */
	protected boolean persist() throws IOException, JsonException {
		logger.debug("Saving document to RemotePipeline..");
		return getRemotePipeline().saveCurrentDocument();
	}

	/**
	 * Handles errors when processing LocalDocuments. The standard
	 * implementation releases the document back to the RemotePipeline and
	 * writes a log message.
	 * 
	 * @param e
	 *            The thrown error.
	 * @throws IOException
	 * @throws JsonException 
	 */
	protected boolean persistError(LocalDocument d, Exception e) throws IOException, JsonException {
		logger.error("Trying to release document due to error in processing", e);
		d.addError(getStageName(), e);
		return getRemotePipeline().saveCurrentDocument();
	}
	
	/**
	 * Process a LocalDocument, changing that in whatever way a AbstractStage
	 * implementation chooses to.
	 * 
	 * @param doc
	 *            The document to be processed
	 * @throws ProcessException
	 *             Whenever a error in processing occurs
	 */
	public abstract void process(LocalDocument doc) throws ProcessException;

	@Override
	public void setUp(RemotePipeline rp, Map<String, Object> properties) throws IllegalArgumentException, IllegalAccessException, IOException {
		super.setUp(rp, properties);
	}


	/**
	 * The thread starts with run(). Use run() to start the execution of this
	 * step in this thread, or start() to start it in a new step (recomended).
	 * The execution can be stopped from another thread by calling the
	 * stopStage() method.
	 * 
	 * The run() method operates by trying to fetch documents through fetch(),
	 * processing by process() and then persisting by persist().
	 * 
	 */
	public void run() {
		
		setContinueRunning(true);

		while (isContinueRunning()) {
			try {
				LocalDocument doc = fetch();
				if (doc == null) {
					Thread.sleep(holdInterval);

				} else {
					try {
						logger.debug("Got new doc " + doc.getID()
								+ " to process.");
						process(doc);
						if(!persist()) {
							LocalDocument ld = new LocalDocument(doc.toJson());
							IOException e = new IOException("Unable to save changes to core");
							if(!getRemotePipeline().markFailed(ld, e)) {
								logger.error("Unable to persist an error to the database", e);
							}
						}
					} catch (ProcessException e) {
						if(failDocumentOnProcessException) {
							getRemotePipeline().markFailed(doc, e);
						} else {
							persistError(doc, e);
						}
					}

				}

			} catch (Exception e) {
				logger.error("Caught exception while running", e);
				Runtime.getRuntime().removeShutdownHook(getShutDownHook());
				System.exit(1);
			}
		}
	}
}
