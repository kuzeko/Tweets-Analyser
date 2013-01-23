package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

@StubAnnotation.ConstantFieldsSecond(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = 1)
public class DictionaryFilterMatch extends MatchStub {

    @Override
    public void match(PactRecord english, PactRecord sentence, Collector<PactRecord> records) throws Exception {
        int englishWords = english.getField(1, PactInteger.class).getValue();
        int totalWords = sentence.getField(3, PactInteger.class).getValue();
        if (englishWords/(double)totalWords > .2) {
            records.collect(sentence);
        }
    }

}
