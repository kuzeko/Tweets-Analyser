package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.io.Serializable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Converts a PactRecord containing one string in to a string tuple
 * a tuple is a English word from the dictionary
 *
 * 0 - word
 */
@StubAnnotation.ConstantFields({})
public class LoadDictionaryMap extends MapStub implements Serializable{

	private static final Log LOG = LogFactory.getLog(LoadDictionaryMap.class);
	private long counter = 0;

	private PactString word = new PactString();

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        word = pr.getField(0, PactString.class);
        StringUtils.toLowerCase(word);
        pr.setField(0, word);
        records.collect(pr);
        if(TweetCleanse.LoadDictionaryMapLog){
            //System.out.printf("LDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.LoadDictionaryMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
