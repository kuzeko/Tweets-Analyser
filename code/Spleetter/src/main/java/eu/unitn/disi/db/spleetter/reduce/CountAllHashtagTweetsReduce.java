/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.match.DictionaryFilterMatch;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag counts the total number of tweets
 *
 * 0 - hashtag
 * 1 - total num tweets
 */
@StubAnnotation.ConstantFields({})
public class CountAllHashtagTweetsReduce extends ReduceStub implements Serializable {
    private static final Log LOG = LogFactory.getLog(CountAllHashtagTweetsReduce.class);
    private long counter = 0;
    private PactInteger numTweets = new PactInteger();

    private PactRecord pr2 = new PactRecord(2);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID = null;
        int sum = 0;


        while (matches.hasNext()) {
            pr = matches.next();
            if(hashtagID == null) {
                hashtagID = pr.getField(1, PactInteger.class);
            }

            sum = pr.getField(2, PactInteger.class).getValue() + sum;

        }

        numTweets.setValue(sum);
        pr2.setField(0, hashtagID);
        pr2.setField(1, numTweets);
        records.collect(pr2);
        if (TweetCleanse.CountAllHashtagTweetsReduceLog) {
            //System.out.printf("TPM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountAllHashtagTweetsReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
