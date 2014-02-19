package eu.unitn.disi.db.spleetter.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweet record with its corresponding date appending it to the end of
 * the record
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - tweet text<br />
 * 3 - num of words in the original tweet<br />
 * 4 - timestamp [h]<br />
 */
@FunctionAnnotation.ConstantFieldsFirst({0, 1, 2, 3})
@FunctionAnnotation.ConstantFieldsSecond({})
public class TweetDateJoin extends JoinFunction {
    private static final Log LOG = LogFactory.getLog(TweetDateJoin.class);
    private long counter = 0;
    private Record output = new Record(5);



    @Override
    public void join(Record tweet, Record tweetDate, Collector<Record> records) throws Exception {
        output.setField(0, tweet.getField(0, LongValue.class));
        output.setField(1, tweet.getField(1, LongValue.class));
        output.setField(2, tweet.getField(2, StringValue.class));
        output.setField(3, tweet.getField(3, IntValue.class));
        output.setField(4, tweetDate.getField(2, StringValue.class));

        records.collect(output);
        if (TweetCleanse.TweetDateJoinLog) {
            //System.out.printf("TDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.TweetDateJoinLog) {
            LOG.fatal(counter);
        }
        super.close();
    }


}
