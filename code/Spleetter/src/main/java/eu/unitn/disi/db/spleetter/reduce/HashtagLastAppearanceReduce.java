package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag returns the last date of appearance
 * 0 - hashtag id
 * 1 - peek timestamp [h]
 */

@FunctionAnnotation.ConstantFields({})
public class HashtagLastAppearanceReduce extends ReduceFunction    implements Serializable {
    private static final Log LOG = LogFactory.getLog(HashtagLastAppearanceReduce.class);
    private long counter = 0;
    private final StringValue timestamp = new StringValue();
    private final Record pr2 = new Record(2);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        IntValue hashtagID = null;
        String maxTimestamp = null;
        String tempTimestamp = null;

        while (matches.hasNext()) {
            pr = matches.next();
            tempTimestamp = pr.getField(0, StringValue.class).getValue();

            if(maxTimestamp == null || tempTimestamp.compareTo(maxTimestamp)>0){
                hashtagID =pr.getField(1, IntValue.class);
                maxTimestamp = pr.getField(0, StringValue.class).getValue();
            }
        }

        timestamp.setValue(maxTimestamp);
        pr2.setField(0, hashtagID);
        pr2.setField(1, timestamp);
        records.collect(pr2);
        if (TweetCleanse.HashtagLastAppearanceReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.HashtagLastAppearanceReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
