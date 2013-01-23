package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.StringUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
 * in the record. The other fields are not modified.
 */
@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = 1)
public class CleanTextMap extends MapStub {
    private PactString tweet = new PactString();
    private Pattern pAt = Pattern.compile("(@[a-zA-Z0-9]+)");
    private Pattern pUrl = Pattern.compile("(((ht|f)tp(s?)\\://)\\S+)");
    private Pattern pHash = Pattern.compile("#");

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        tweet = pr.getField(2, PactString.class);
        String text = tweet.getValue();

        Matcher matchAt  = null;
        Matcher matchUrl  = null;
        Matcher matchHash  = null;

        matchAt = pAt.matcher(text);
        text = matchAt.replaceAll("");
        matchUrl = pUrl.matcher(text);
        text = matchUrl.replaceAll("");
        matchHash = pHash.matcher(text);
        text = matchHash.replaceAll("");
        text = StringUtils.removeStopwords(text);
        if (text != null) {
            tweet.setValue(text);
            pr.setField(2, tweet);
            records.collect(pr);
        }
    }
}
