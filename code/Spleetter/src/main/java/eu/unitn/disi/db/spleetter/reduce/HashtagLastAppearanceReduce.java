package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * For each hashtag returns the last date of appearance
 * 0 - hashtag id
 * 1 - peek timestamp [h]
 */

@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class HashtagLastAppearanceReduce extends ReduceStub {

    private final PactString timestamp = new PactString();
    private final PactRecord pr2 = new PactRecord(2);

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        PactInteger hashtagID = null;
        String maxTimestamp = null;
        String tempTimestamp = null;

        while (matches.hasNext()) {
            pr = matches.next();
            tempTimestamp = pr.getField(0, PactString.class).getValue();

            if(maxTimestamp == null || tempTimestamp.compareTo(maxTimestamp)>0){
                hashtagID =pr.getField(1, PactInteger.class);
                maxTimestamp = pr.getField(0, PactString.class).getValue();
            }
        }

        timestamp.setValue(maxTimestamp);
        pr2.setField(0, hashtagID);
        pr2.setField(1, timestamp);
        records.collect(pr2);
    }
}
