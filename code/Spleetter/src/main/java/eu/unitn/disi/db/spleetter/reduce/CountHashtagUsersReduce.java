/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
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
@FunctionAnnotation.ConstantFields({0,1})
public class CountHashtagUsersReduce extends ReduceFunction     implements Serializable{
    private static final Log LOG = LogFactory.getLog(CountHashtagUsersReduce.class);
    private long counter = 0;
    private IntValue numDistinctUsers = new IntValue();
    private HashSet<LongValue> users = new HashSet<LongValue>();
    private Record pr2 = new Record(3);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        IntValue hashtagID = null;
        IntValue hashtagID2 = null;
        LongValue userID;

        users.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            userID = pr.getField(2, LongValue.class);

            //TO BE REMOVED
            hashtagID2 = pr.getField(1, IntValue.class);
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
        pr2.setField(0, pr.getField(0, StringValue.class));
        pr2.setField(1, pr.getField(1, IntValue.class));
        pr2.setField(2, numDistinctUsers);
        records.collect(pr2);
        if (TweetCleanse.CountHashtagUsersReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
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
