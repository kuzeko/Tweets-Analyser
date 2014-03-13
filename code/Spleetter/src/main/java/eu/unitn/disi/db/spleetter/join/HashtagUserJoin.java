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
 * Joins the tweets' authors to the hashtags present in their tweets
 * appending them to the end of the record
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - user id
 *
 */
@FunctionAnnotation.ConstantFieldsFirst({})
public class HashtagUserJoin extends JoinFunction {
    private Record pr2 = new Record(3);
    private static final Log LOG = LogFactory.getLog(HashtagUserJoin.class);
    private long counter = 0;

    @Override
    public void join(Record userTweet, Record hashtagRecord, Collector<Record> records) throws Exception {
        pr2.setField(0, userTweet.getField(2, StringValue.class));
        pr2.setField(1, hashtagRecord.getField(2, IntValue.class));
        pr2.setField(2, userTweet.getField(1, LongValue.class));
        records.collect(pr2);
        if(TweetCleanse.HashtagUserJoinLog){
            //System.out.printf("HPM out %s\n", pr2.getField(0, StringValue.class));
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.HashtagUserJoinLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
