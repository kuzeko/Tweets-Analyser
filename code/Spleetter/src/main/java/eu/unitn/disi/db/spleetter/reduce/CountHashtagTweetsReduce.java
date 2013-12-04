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
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each timestamp, for each hashtag counts the tweets
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - num tweets
 */
@StubAnnotation.ConstantFields({0,1})
public class CountHashtagTweetsReduce extends ReduceStub implements Serializable{
    private static final Log LOG = LogFactory.getLog(CountHashtagTweetsReduce.class);
    private long counter = 0;
    private PactInteger numTweets = new PactInteger();
    private PactRecord pr2 = new PactRecord(3);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID = null;
        PactInteger hashtagID2 = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum = sum + 1;

            //TO BE REMOVED
            hashtagID2 = pr.getField(1, PactInteger.class);
            if(hashtagID==null){
                hashtagID = hashtagID2;
            } else if(!hashtagID.equals(hashtagID2)){
                throw new IllegalStateException("WAT!?!? Different hashtagIDs");
            }
            //END TO BE REMOVED AFTER DEBUG


        }

        numTweets.setValue(sum);

        pr2.setField(0, pr.getField(0, PactString.class));
        pr2.setField(1, pr.getField(1, PactInteger.class));
        pr2.setField(2, numTweets);
        records.collect(pr2);

        if (TweetCleanse.CountHashtagTweetsReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountHashtagTweetsReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
