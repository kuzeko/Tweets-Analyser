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
 * For each timestamp counts the tweets
 *
 * 0 - timestamp [h]
 * 1 - num tweets
 */
@FunctionAnnotation.ConstantFields({})
public class CountTweetsHourlyReduce extends ReduceFunction{
    private static final Log LOG = LogFactory.getLog(CountTweetsHourlyReduce.class);
    private long counter = 0;
    private IntValue numTweets = new IntValue();
    private Record pr2 = new Record(2);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum = sum + 1;
        }

        numTweets.setValue(sum);

        pr2.setField(0, pr.getField(4, StringValue.class));
        pr2.setField(1, numTweets);
        records.collect(pr2);

        if (TweetCleanse.CountTweetsHourlyReduceLog) {
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountTweetsHourlyReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
