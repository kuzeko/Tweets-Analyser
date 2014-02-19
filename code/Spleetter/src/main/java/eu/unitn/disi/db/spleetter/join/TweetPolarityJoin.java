package eu.unitn.disi.db.spleetter.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the cleaned english tweets with their correspondent polarities
 * appending them to the end of the record
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - tweet text<br />
 * 3 - num of words in the original tweet<br />
 * 4 - timestamp [h]<br />
 * 5 - negative polarity<br />
 * 6 - positive polarity<br />
 */
@FunctionAnnotation.ConstantFieldsFirst({0, 2, 3, 4})
public class TweetPolarityJoin extends JoinFunction{

    private static final Log LOG = LogFactory.getLog(TweetPolarityJoin.class);
    long counter = 0;
    private Record output = new Record(7);

    @Override
    public void join(Record englishTweet, Record polarityValue, Collector<Record> records) throws Exception {
        output.setField(0, polarityValue.getField(0, LongValue.class));
        output.setField(1, polarityValue.getField(1, LongValue.class));
        output.setField(2, englishTweet.getField(2, StringValue.class));
        output.setField(3, englishTweet.getField(3, IntValue.class));
        output.setField(4, englishTweet.getField(4, StringValue.class));
        output.setField(5, polarityValue.getField(2, DoubleValue.class));
        output.setField(6, polarityValue.getField(3, DoubleValue.class));

        records.collect(output);
        if (TweetCleanse.TweetPolarityJoinLog) {
            //System.out.printf("TPM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.TweetPolarityJoinLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
