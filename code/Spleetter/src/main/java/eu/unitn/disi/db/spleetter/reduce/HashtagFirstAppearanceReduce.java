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
 * For each hashtag returns the first date of appearance
 * 0 - hashtag id
 * 1 - peek timestamp [h]
 */

@FunctionAnnotation.ConstantFields({})
public class HashtagFirstAppearanceReduce extends ReduceFunction{
    private static final Log LOG = LogFactory.getLog(HashtagFirstAppearanceReduce.class);
    private long counter = 0;
    private final StringValue timestamp = new StringValue();
    private final Record pr2 = new Record(2);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        IntValue hashtagID = null;
        String minTimestamp = null;
        String tempTimestamp = null;

        while (matches.hasNext()) {
            pr = matches.next();
            tempTimestamp = pr.getField(0, StringValue.class).getValue();

            if(minTimestamp == null || tempTimestamp.compareTo(minTimestamp) <0){
                hashtagID =pr.getField(1, IntValue.class);
                minTimestamp = pr.getField(0, StringValue.class).getValue();
            }
        }

        timestamp.setValue(minTimestamp);
        pr2.setField(0, hashtagID);
        pr2.setField(1, timestamp);
        records.collect(pr2);
        if (TweetCleanse.HashtagFirstAppearanceReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.HashtagFirstAppearanceReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
