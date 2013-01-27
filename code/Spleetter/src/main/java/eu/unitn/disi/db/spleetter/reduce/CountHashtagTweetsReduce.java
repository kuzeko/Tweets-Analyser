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
import java.util.HashMap;
import java.util.Iterator;

/**
 * For each timestamp, for each hashtag counts the tweets
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - num tweets
 */
@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.INPUTCARD)
public class CountHashtagTweetsReduce extends ReduceStub {

    private PactInteger numTweets = new PactInteger();
    private PactInteger pactHashtagID = new PactInteger();
    private HashMap<Integer, PactInteger> hashtagTweets = new HashMap<Integer, PactInteger>();
    private PactRecord pr2 = new PactRecord(3);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        Integer hashtagID;
        int sum = 0;
        hashtagTweets.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            hashtagID = pr.getField(1, PactInteger.class).getValue();

            // System.out.println("cccc" + hashtagID + "/");

            if (hashtagTweets.containsKey(hashtagID)) {
                numTweets = hashtagTweets.get(hashtagID);
                sum = numTweets.getValue() + 1;
                numTweets.setValue(sum);
                hashtagTweets.put(hashtagID, numTweets);
            } else {
                numTweets = new PactInteger();
                numTweets.setValue(1);
                hashtagTweets.put(hashtagID,numTweets);
            }
        }


        for (Integer hID : hashtagTweets.keySet()) {
            // System.out.println("hhh"+hID);
            numTweets = hashtagTweets.get(hID);
            pactHashtagID.setValue(hID);

            pr2.setField(0, pr.getField(0, PactString.class));
            pr2.setField(1, pactHashtagID);
            pr2.setField(2, numTweets);
            records.collect(pr2);
        }

    }
}
