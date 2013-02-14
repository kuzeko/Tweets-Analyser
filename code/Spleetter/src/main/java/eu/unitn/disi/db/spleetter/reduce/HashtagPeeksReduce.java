package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
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

@StubAnnotation.ConstantFields(fields = {2})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.INPUTCARD)
public class HashtagPeeksReduce extends ReduceStub {
    private static final Log LOG = LogFactory.getLog(HashtagPeeksReduce.class);
    private long counter = 0;
    private final PactInteger peekCount = new PactInteger();
    private final HashSet<PactString> timestamps = new HashSet<PactString>();
    private final PactRecord pr2 = new PactRecord(3);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID = null;
        int count = 0;
        int maxValue = 0;
        timestamps.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            count = pr.getField(2, PactInteger.class).getValue();
            if(count>maxValue){
                maxValue = count;
                hashtagID =pr.getField(1, PactInteger.class);
                timestamps.clear();
                timestamps.add(pr.getField(0, PactString.class));
            } else if(count == maxValue){
                timestamps.add(pr.getField(0, PactString.class));
            }
        }

        if(hashtagID!=null){
            peekCount.setValue(maxValue);
            for (PactString timestamp : timestamps) {
                pr2.setField(0, hashtagID);
                pr2.setField(1, timestamp);
                pr2.setField(2, peekCount);
                records.collect(pr2);
                if (TweetCleanse.HashtagPeeksReduceLog) {
                    //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
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
