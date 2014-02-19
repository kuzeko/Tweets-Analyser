package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag emits time of peeks
 * 0 - hashtag id
 * 1 - peek timestamp [h]
 * 2 - tweets count
 */

@FunctionAnnotation.ConstantFields({2})
public class HashtagPeeksReduce extends ReduceFunction{
    private static final Log LOG = LogFactory.getLog(HashtagPeeksReduce.class);
    private long counter = 0;
    private final IntValue peekCount = new IntValue();
    private final HashSet<StringValue> timestamps = new HashSet<StringValue>();
    private final Record pr2 = new Record(3);

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        IntValue hashtagID = null;
        int count = 0;
        int maxValue = 0;
        timestamps.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            count = pr.getField(2, IntValue.class).getValue();
            if(count>maxValue){
                maxValue = count;
                hashtagID =pr.getField(1, IntValue.class);
                timestamps.clear();
                timestamps.add(pr.getField(0, StringValue.class));
            } else if(count == maxValue){
                timestamps.add(pr.getField(0, StringValue.class));
            }
        }

        if(hashtagID!=null){
            peekCount.setValue(maxValue);
            for (StringValue timestamp : timestamps) {
                pr2.setField(0, hashtagID);
                pr2.setField(1, timestamp);
                pr2.setField(2, peekCount);
                records.collect(pr2);
                if (TweetCleanse.HashtagPeeksReduceLog) {
                    //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
                    this.counter++;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.HashtagPeeksReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
