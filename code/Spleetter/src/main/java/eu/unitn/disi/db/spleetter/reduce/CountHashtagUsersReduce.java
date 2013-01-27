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
import java.util.HashSet;
import java.util.Iterator;

/**
 * For each timestamp, for each hashtag count the number of distinct users
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - num distinct users
 */
@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.INPUTCARD)
public class CountHashtagUsersReduce extends ReduceStub {

    private PactInteger numDistinctUsers = new PactInteger();
    private HashMap<PactInteger, HashSet<PactInteger>> hashtagUsers = new HashMap<PactInteger, HashSet<PactInteger>>();
    private PactRecord pr2 = new PactRecord(3);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID;
        PactInteger userID;
        HashSet<PactInteger> usersSet;
                
        hashtagUsers.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            hashtagID = pr.getField(1, PactInteger.class);
            userID = pr.getField(2, PactInteger.class);
            if (hashtagUsers.containsKey(hashtagID)) {
                hashtagUsers.get(hashtagID).add(userID);
            } else {
                usersSet = new HashSet<PactInteger>();
                usersSet.add(userID);
                hashtagUsers.put(hashtagID, usersSet);
            }
        }


        for (PactInteger hID : hashtagUsers.keySet()) {
            numDistinctUsers.setValue(hashtagUsers.get(hID).size());
            //System.out.printf("Tweet: %d, english words: %d\n", pr.getField(0, PactLong.class).getValue(), sum);
            pr2.setField(0, pr.getField(0, PactString.class));
            pr2.setField(1, hID);
            pr2.setField(2, numDistinctUsers);
            records.collect(pr2);
        }

    }
}
