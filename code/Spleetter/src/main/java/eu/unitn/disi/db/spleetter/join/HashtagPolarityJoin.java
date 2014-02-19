package eu.unitn.disi.db.spleetter.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins tweets records polarities with their correspondent hashtags
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - negative polarity
 * 3 - positive polarity
 */
@FunctionAnnotation.ConstantFieldsFirst({})
@FunctionAnnotation.ConstantFieldsSecond({})
public class HashtagPolarityJoin extends JoinFunction{
    private static final Log LOG = LogFactory.getLog(DictionaryFilterJoin.class);
    private long counter = 0;

    private Record pr2 = new Record(4);

    @Override
    public void join(Record tweetRecord, Record hashtagRecord, Collector<Record> records) throws Exception {

        pr2.setField(0, tweetRecord.getField(2, StringValue.class));
        pr2.setField(1, hashtagRecord.getField(2, IntValue.class));
        pr2.setField(2, tweetRecord.getField(3, DoubleValue.class));
        pr2.setField(3, tweetRecord.getField(4, DoubleValue.class));
        records.collect(pr2);
        if(TweetCleanse.HashtagPolarityJoinLog){
            //System.out.printf("HPM out %s\n", pr2.getField(0, StringValue.class));
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.HashtagPolarityJoinLog){
            LOG.fatal(counter);
        }
    	super.close();
    }


}
