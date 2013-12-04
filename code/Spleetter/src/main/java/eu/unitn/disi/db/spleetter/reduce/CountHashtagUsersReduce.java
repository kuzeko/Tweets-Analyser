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
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each timestamp, for each hashtag count the number of distinct users
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - num distinct users
 */
@StubAnnotation.ConstantFields({0,1})
public class CountHashtagUsersReduce extends ReduceStub {
    private static final Log LOG = LogFactory.getLog(CountHashtagUsersReduce.class);
    private long counter = 0;
    private PactInteger numDistinctUsers = new PactInteger();
    private HashSet<PactInteger> users = new HashSet<PactInteger>();
    private PactRecord pr2 = new PactRecord(3);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID = null;
        PactInteger hashtagID2 = null;
        PactInteger userID;

        users.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            userID = pr.getField(2, PactInteger.class);

            //TO BE REMOVED
            hashtagID2 = pr.getField(1, PactInteger.class);
            if(hashtagID==null){
                hashtagID = hashtagID2;
            } else if(!hashtagID.equals(hashtagID2)){
                throw new IllegalStateException("WAT!?!? Different hashtagIDs");
            }
            //END TO BE REMOVED AFTER DEBUG

            users.add(userID);
        }

        if(users == null ){
            throw new IllegalStateException("WAT!?!?");
        }

        numDistinctUsers.setValue(users.size());
        pr2.setField(0, pr.getField(0, PactString.class));
        pr2.setField(1, pr.getField(1, PactInteger.class));
        pr2.setField(2, numDistinctUsers);
        records.collect(pr2);
        if (TweetCleanse.CountHashtagUsersReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountHashtagUsersReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
