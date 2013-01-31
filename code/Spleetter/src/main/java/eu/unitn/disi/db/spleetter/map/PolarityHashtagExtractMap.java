package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Filters the tweet record keeping only the tweet id, user id, timestamp and polarity
 * 0 - tweet id
 * 1 - user id
 * 2 - timestamp [h]
 * 3 - neg polarity
 * 4 - pos polarity
 */

@StubAnnotation.ConstantFields(fields = {0,1})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class PolarityHashtagExtractMap extends MapStub {
    private PactRecord pr2 = new PactRecord(1);

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        pr2.setField(0, pr.getField(0, PactLong.class ));
        pr2.setField(1, pr.getField(1, PactInteger.class ));
        pr2.setField(2, pr.getField(4, PactString.class ));
        pr2.setField(3, pr.getField(5, PactDouble.class ));
        pr2.setField(4, pr.getField(6, PactDouble.class ));
        records.collect(pr2);
        System.out.printf("PHM %s\n", pr2.getField(0, PactLong.class));
//        System.out.printf("%d,%d,%s,%d,%s\n",
//                output.getField(0, PactLong.class).getValue(),
//                output.getField(1, PactInteger.class).getValue(),
//                output.getField(2, PactString.class).getValue(),
//                output.getField(3, PactInteger.class).getValue(),
//                output.getField(4, PactString.class).getValue()
//                );


    }
}
