package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reads the text from a cleaned tweet record and splits it into single words
 *
 * 0 - word<br /> 1 - tweet id<br />
 *
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
public class SplitSentenceMap extends MapStub {

    private long counter = 0;
    private static final Log LOG = LogFactory.getLog(SplitSentenceMap.class);
    private List<String> stopwordsList;
    private PactString line;
    private PactLong tid;
    private PactString word = new PactString();
    private StringUtils.WhitespaceTokenizer tokenizer = new StringUtils.WhitespaceTokenizer();
    private PactRecord output = new PactRecord(3);

    @Override
    public void open(Configuration parameters) {
        this.stopwordsList = Arrays.asList(SplitSentenceMap.stopwords);
    }

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        tid = pr.getField(0, PactLong.class);
        line = pr.getField(2, PactString.class);
        tokenizer.setStringToTokenize(line);
        while (tokenizer.next(word)) {
            String wordString = word.getValue().toLowerCase();
            if (!stopwordsList.contains(wordString)) {
                output.setField(0, word);
                output.setField(1, tid);

                records.collect(output);
                if (TweetCleanse.SplitSentenceMapLog) {
                    //System.out.printf("SSM out\n");
                    this.counter++;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.SplitSentenceMapLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
    private static final String[] stopwords = {"a",
        "about",
        "above",
        "after",
        "again",
        "against",
        "all",
        "am",
        "an",
        "and",
        "any",
        "are",
        "aren't",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "below",
        "between",
        "both",
        "but",
        "by",
        "can't",
        "cannot",
        "could",
        "couldn't",
        "did",
        "didn't",
        "do",
        "does",
        "doesn't",
        "doing",
        "don't",
        "down",
        "during",
        "each",
        "few",
        "for",
        "from",
        "further",
        "had",
        "hadn't",
        "has",
        "hasn't",
        "have",
        "haven't",
        "having",
        "he",
        "he'd",
        "he'll",
        "he's",
        "her",
        "here",
        "here's",
        "hers",
        "herself",
        "him",
        "himself",
        "his",
        "how",
        "how's",
        "i",
        "i'd",
        "i'll",
        "i'm",
        "i've",
        "if",
        "in",
        "into",
        "is",
        "isn't",
        "it",
        "it's",
        "its",
        "itself",
        "let's",
        "me",
        "more",
        "most",
        "mustn't",
        "my",
        "myself",
        "no",
        "nor",
        "not",
        "of",
        "off",
        "on",
        "once",
        "only",
        "or",
        "other",
        "ought",
        "our",
        "ours ",
        "ourselves",
        "out",
        "over",
        "own",
        "same",
        "shan't",
        "she",
        "she'd",
        "she'll",
        "she's",
        "should",
        "shouldn't",
        "so",
        "some",
        "such",
        "than",
        "that",
        "that's",
        "the",
        "their",
        "theirs",
        "them",
        "themselves",
        "then",
        "there",
        "there's",
        "these",
        "they",
        "they'd",
        "they'll",
        "they're",
        "they've",
        "this",
        "those",
        "through",
        "to",
        "too",
        "under",
        "until",
        "up",
        "very",
        "was",
        "wasn't",
        "we",
        "we'd",
        "we'll",
        "we're",
        "we've",
        "were",
        "weren't",
        "what",
        "what's",
        "when",
        "when's",
        "where",
        "where's",
        "which",
        "while",
        "who",
        "who's",
        "whom",
        "why",
        "why's",
        "with",
        "won't",
        "would",
        "wouldn't",
        "you",
        "you'd",
        "you'll",
        "you're",
        "you've",
        "your",
        "yours",
        "yourself",
        "yourselves"};
}
