package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Joins tweets records polarities with their correspondent hashtags
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - negative polarity
 * 3 - positive polarity
 */
@StubAnnotation.ConstantFieldsFirst(fields = {})
@StubAnnotation.ConstantFieldsSecond(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class HashtagPolarityMatch extends MatchStub {
    private PactRecord pr2 = new PactRecord(4);

    @Override
    public void match(PactRecord tweetRecord, PactRecord hashtagRecord, Collector<PactRecord> records) throws Exception {

        pr2.setField(0, tweetRecord.getField(2, PactString.class));
        pr2.setField(1, hashtagRecord.getField(1, PactInteger.class));
        pr2.setField(2, tweetRecord.getField(3, PactDouble.class));
        pr2.setField(3, tweetRecord.getField(4, PactDouble.class));
        records.collect(pr2);
        System.out.printf("HPM %s\n", pr2.getField(0, PactString.class));

    }

}
