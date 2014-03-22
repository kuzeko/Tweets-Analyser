/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag counts the total number of tweets
 *
 * 0 - hashtag
 * 1 - total num tweets appearances
 */
@FunctionAnnotation.ConstantFields({})
public class CountHashtagAppearancesReduce extends ReduceFunction {

    private static final Log LOG = LogFactory.getLog(CountHashtagAppearancesReduce.class);
    private long counter = 0;
    private IntValue numTweets = new IntValue();
    private int numThreshold = 0;
    private Record pr2 = new Record(2);

    @Override
    public void open(Configuration parameters) {
        this.numThreshold = Integer.parseInt(parameters.getString(TweetCleanse.APPEARANCE_TRESHOLD, "1"));
    }

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        IntValue hashtagID = null;
        int sum = 0;


        while (matches.hasNext()) {
            pr = matches.next();
            sum = pr.getField(2, IntValue.class).getValue() + sum;
        }
        if (sum >= this.numThreshold) {
            numTweets.setValue(sum);

            pr2.setField(0, pr.getField(1, IntValue.class));
            pr2.setField(1, numTweets);
            records.collect(pr2);
            if (TweetCleanse.CountHashtagAppearancesReduceLog) {
                //System.out.printf("TPM out\n");
                this.counter++;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountHashtagAppearancesReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
