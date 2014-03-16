package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Takes as input a tweet record and return is with a clean text
 * Strips out URLs, Hashtag signs and @Mentions
 * 0 - tweet id
 * 1 - user id
 * 2 - text
 * 3 - number of words in original tweet
 * 4 - timestamp [h]
 */
@FunctionAnnotation.ConstantFields({0,1,3,4})
public class CleanTextMap extends MapFunction{

    private static final Log LOG = LogFactory.getLog(CleanTextMap.class);
    private long counter = 0;

    private StringValue tweet = new StringValue();
    private Pattern pAt = Pattern.compile("(@[a-zA-Z0-9]+)");
    private Pattern pUrl = Pattern.compile("(((ht|f)tp(s?)\\://)\\S+)");
    private Pattern pHash = Pattern.compile("#");

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        tweet = pr.getField(2, StringValue.class);
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
        if (text != null && !text.isEmpty()) {
            text = text.toLowerCase();
            tweet.setValue(text);
            pr.setField(2, tweet);
            records.collect(pr);
            if(TweetCleanse.CleanTextMapLog){
              //System.out.printf("CTM out\n");
              this.counter++;
            }

        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.CleanTextMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
