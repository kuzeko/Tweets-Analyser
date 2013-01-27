package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.utils.StringUtils;

/**
 * Converts a PactRecord containing one string in to a string tuple
 * a tuple is a English word from the dictionary
 * 0 - word
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class LoadDictionaryMap extends MapStub {
    private PactString word = new PactString();

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        word = pr.getField(0, PactString.class);
        StringUtils.toLowerCase(word);
        pr.setField(0, word);
        records.collect(pr);
    }
}
