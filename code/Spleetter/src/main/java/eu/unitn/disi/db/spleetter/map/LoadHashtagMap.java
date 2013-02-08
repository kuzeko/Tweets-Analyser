package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;


/**
 * Converts a PactRecord containing one string in to multiple integer pairs.
 * 0 - tweet id
 * 1 - hashtag id
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class LoadHashtagMap extends MapStub {

    private PactRecord outputRecord = new PactRecord();
    private PactLong tid = new PactLong();
    private PactInteger hid = new PactInteger();
    private final PactString line = new PactString();

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        line.setValue(pr.getField(0, PactString.class));
        String[] splittedLine = line.toString().split(",");

        tid.setValue(Long.parseLong(splittedLine[0]));
        hid.setValue(Integer.parseInt(splittedLine[1]));

        outputRecord.setField(0, tid);
        outputRecord.setField(1, hid);

        if(TweetCleanse.LoadHashtagMapLog){
            System.out.printf("LHM out\n");
        }


        records.collect(outputRecord);
    }
}
