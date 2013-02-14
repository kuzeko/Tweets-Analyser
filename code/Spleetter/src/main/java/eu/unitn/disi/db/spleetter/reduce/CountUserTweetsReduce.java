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
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each user counts the number of tweets
 * 0 - user ID
 * 1 - number of tweets
 */

@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class CountUserTweetsReduce extends ReduceStub {
    private static final Log LOG = LogFactory.getLog(CountUserTweetsReduce.class);
    private long counter = 0;
    private PactInteger numTweets = new PactInteger();
    private PactRecord pr2 = new PactRecord(2);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum ++;
        }

        numTweets.setValue(sum);
        pr2.setField(0, pr.getField(0, PactInteger.class));
        pr2.setField(1, numTweets);
        records.collect(pr2);
        if (TweetCleanse.CountUserTweetsReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountUserTweetsReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
