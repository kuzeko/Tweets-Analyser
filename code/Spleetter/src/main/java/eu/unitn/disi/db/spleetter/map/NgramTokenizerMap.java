package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reads the text from a cleaned tweet record and splits it into n-grams
 * This does not remove stopwords
 *
 * 0 - ngram<br />
 * 1 - tweet id<br />
 * 2 - user id<br />
 * 3 - tweet date [h]<br />
 */
@FunctionAnnotation.ConstantFields({})
public class NgramTokenizerMap extends MapFunction {

    private long counter = 0;
    private static final Log LOG = LogFactory.getLog(NgramTokenizerMap.class);
    private int nLength = 1;
    private LongValue tid;
    private LongValue uid;
    private StringValue ngramField = new StringValue();
    private StringValue line;
    private StringValue tdate;
    private Record output = new Record(4);
    private Pattern sentenceSepPattern = Pattern.compile("([^\\p{L}^\\p{Digit}^\" \"^\']+)");

    /**
     * Reads the filter literals from the configuration.
     *
     * @see
     * eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
     */
    @Override
    public void open(Configuration parameters) {
        this.nLength = parameters.getInteger(TweetCleanse.N_GRAM_LENGTH, 1);
    }

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {

        tid = pr.getField(0, LongValue.class);
        uid = pr.getField(1, LongValue.class);
        line = pr.getField(2, StringValue.class);
        tdate = pr.getField(4, StringValue.class);

        String[] splittedSentence = sentenceSepPattern.split(line);

        for (String sentence : splittedSentence) {
            line.setValue(sentence.trim());

            List<String> ngrams = StringUtils.ngrams(this.nLength, line);

            for (String ngram : ngrams) {
                ngramField.setValue(ngram);
                output.setField(0, ngramField);
                output.setField(1, tid);
                output.setField(2, uid);
                output.setField(3, tdate);

                records.collect(output);

                if (TweetCleanse.NgramTokenizerMapLog) {
                    //System.out.printf("SSM out\n");
                    this.counter++;
                }
            }
        }

    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.NgramTokenizerMapLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
