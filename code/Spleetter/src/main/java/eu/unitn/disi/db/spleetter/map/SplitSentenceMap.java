package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.StringUtils;

@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class SplitSentenceMap extends MapStub {
    private PactString line;
    private PactLong tid; 
    private PactString word = new PactString();
    private StringUtils.WhitespaceTokenizer tokenizer = new StringUtils.WhitespaceTokenizer();
    private PactRecord output = new PactRecord();

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        tid = pr.getField(0, PactLong.class);
        line = pr.getField(2, PactString.class);
        tokenizer.setStringToTokenize(line);
        while (tokenizer.next(word)) {
            output.setField(0, word);
            output.setField(1, tid);
            records.collect(output);
        }
    }
}