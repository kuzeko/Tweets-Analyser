package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweets' first and last appearance date
 *
 * 0 - hashtag id
 * 1 - first timestamp [h]
 * 2 - last timestamp [h]
 *
 */
@StubAnnotation.ConstantFieldsFirst({1})
@StubAnnotation.ConstantFieldsSecond({1})
public class HashtagLifespanMatch extends MatchStub {
    private static final Log LOG = LogFactory.getLog(DictionaryFilterMatch.class);
    private long counter = 0;

    private PactRecord pr2 = new PactRecord(3);

    @Override
    public void match(PactRecord first, PactRecord last, Collector<PactRecord> records) throws Exception {

        pr2.setField(0, first.getField(0, PactInteger.class));
        pr2.setField(1, first.getField(1, PactString.class));
        pr2.setField(2, last.getField(1, PactString.class));
        records.collect(pr2);
        if(TweetCleanse.HashtagLifespanMatchLog){
            this.counter++;
        }

    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.HashtagLifespanMatchLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
