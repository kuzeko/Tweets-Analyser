package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reads the text from a cleaned tweet record and splits it into single words
 *
 * 0 - word<br />
 * 1 - tweet id<br />
 *
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class SplitSentenceMap extends MapStub {

    private long counter = 0;
    private static final Log LOG = LogFactory.getLog(SplitSentenceMap.class);

    private PactString line;
    private PactLong tid;
    private PactString word = new PactString();
    private StringUtils.WhitespaceTokenizer tokenizer = new StringUtils.WhitespaceTokenizer();
    private PactRecord output = new PactRecord(3);

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        tid = pr.getField(0, PactLong.class);
        line = pr.getField(2, PactString.class);
        tokenizer.setStringToTokenize(line);
        while (tokenizer.next(word)) {
            output.setField(0, word);
            output.setField(1, tid);

            records.collect(output);
            if(TweetCleanse.SplitSentenceMapLog){
              //System.out.printf("SSM out\n");
              this.counter++;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.SplitSentenceMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
