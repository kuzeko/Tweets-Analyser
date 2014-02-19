/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each user counts the number of tweets
 * 0 - user ID
 * 1 - number of tweets
 */

@FunctionAnnotation.ConstantFields({0})
public class CountUserTweetsReduce extends ReduceFunction {
    private static final Log LOG = LogFactory.getLog(CountUserTweetsReduce.class);
    private long counter = 0;
    private IntValue numTweets = new IntValue();
    private Record pr2 = new Record(2);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum ++;
        }

        numTweets.setValue(sum);
        pr2.setField(0, pr.getField(0, IntValue.class));
        pr2.setField(1, numTweets);
        records.collect(pr2);
        if (TweetCleanse.CountUserTweetsReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
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
