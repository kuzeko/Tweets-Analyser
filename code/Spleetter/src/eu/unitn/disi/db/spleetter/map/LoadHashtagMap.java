package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.StringUtils;

@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class LoadHashtagMap extends MapStub {
    private PactRecord outputRecord = new PactRecord();
    private PactLong tid = new PactLong();
    private PactInteger hid = new PactInteger();
    
    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        tid = pr.getField(0, PactLong.class);
        hid = pr.getField(1, PactInteger.class);
        
        outputRecord.setField(0, tid);
        outputRecord.setField(1, hid);
        records.collect(outputRecord);
    }
}
