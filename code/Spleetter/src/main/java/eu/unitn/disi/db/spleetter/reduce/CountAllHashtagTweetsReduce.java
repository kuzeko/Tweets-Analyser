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
import java.util.Iterator;

/**
 * For each hashtag counts the total number of tweets
 *
 * 0 - hashtag
 * 1 - total num tweets
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class CountAllHashtagTweetsReduce extends ReduceStub {

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

            sum = pr.getField(2, PactInteger.class).getValue() + numTweets.getValue();
            numTweets.setValue(sum);
        }

        pr2.setField(0, hashtagID);
        pr2.setField(1, numTweets);
        records.collect(pr2);

    }
}
