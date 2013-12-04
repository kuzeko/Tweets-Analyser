package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag returns the first date of appearance
 * 0 - hashtag id
 * 1 - peek timestamp [h]
 */

@StubAnnotation.ConstantFields({})
public class HashtagFirstAppearanceReduce extends ReduceStub implements Serializable{
    private static final Log LOG = LogFactory.getLog(HashtagFirstAppearanceReduce.class);
    private long counter = 0;
    private final PactString timestamp = new PactString();
    private final PactRecord pr2 = new PactRecord(2);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID = null;
        String minTimestamp = null;
        String tempTimestamp = null;

        while (matches.hasNext()) {
            pr = matches.next();
            tempTimestamp = pr.getField(0, PactString.class).getValue();

            if(minTimestamp == null || tempTimestamp.compareTo(minTimestamp) <0){
                hashtagID =pr.getField(1, PactInteger.class);
                minTimestamp = pr.getField(0, PactString.class).getValue();
            }
        }

        timestamp.setValue(minTimestamp);
        pr2.setField(0, hashtagID);
        pr2.setField(1, timestamp);
        records.collect(pr2);
        if (TweetCleanse.HashtagFirstAppearanceReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
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
