/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
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
@FunctionAnnotation.ConstantFields({0,1})
public class CountHashtagTweetsReduce extends ReduceFunction{
    private static final Log LOG = LogFactory.getLog(CountHashtagTweetsReduce.class);
    private long counter = 0;
    private IntValue numTweets = new IntValue();
    private Record pr2 = new Record(3);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        IntValue hashtagID = null;
        IntValue hashtagID2 = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum = sum + 1;

            //TO BE REMOVED
            hashtagID2 = pr.getField(1, IntValue.class);
            if(hashtagID==null){
                hashtagID = hashtagID2;
            } else if(!hashtagID.equals(hashtagID2)){
                throw new IllegalStateException("WAT!?!? Different hashtagIDs");
            }
            //END TO BE REMOVED AFTER DEBUG


        }

        numTweets.setValue(sum);

        pr2.setField(0, pr.getField(0, StringValue.class));
        pr2.setField(1, pr.getField(1, IntValue.class));
        pr2.setField(2, numTweets);
        records.collect(pr2);

        if (TweetCleanse.CountHashtagTweetsReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
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
