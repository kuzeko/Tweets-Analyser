package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Analyze the text of tweet and if it matches the RegExp
 * flags it as probable spam
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - is spam<br />
 */
@FunctionAnnotation.ConstantFields({0, 1})
public class SpamFlagMap extends MapFunction {

    private long counter = 0;

    private StringValue tweet = new StringValue();
    private IntValue isSpam = new IntValue();
    private Record pr2 = new Record(3);

    private static final Log LOG = LogFactory.getLog(SpamFlagMap.class);

    static Pattern spam_earn = Pattern.compile(".*\\$[0-9\\.\\,]+s*\\s*[aion/]{1,2}\\s+.*|.*(earn(ed)*|make|made)\\s+\\$[0-9\\.\\,]+.*|.*(mak[e]*|earn)(ing)* money.*");
    static Pattern spam_watch = Pattern.compile(".*watch(ed|ing)*.{0,60}( movie| at| on| free| onlin[e]*| full| here| pc| see)+[\\.\\,\\-\\!\\?: ]+http.*");
    static Pattern spam_download = Pattern.compile(".*(giveaway|download(ed)*|dvd[^s]|blu\\-ray|xvid|x264|subtitle).*");
    static Pattern spam_win = Pattern.compile(".*(just enter(ed)* |just voted |just won |scored|quiz|try it|vote too|webcam).*");
    static Pattern spam_free = Pattern.compile(".*\\$[0-9\\.\\,]+\\s*(free|gift).*|.*win\\s*\\$[0-9\\.\\,]+.*");

    static Matcher match;


    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        tweet = pr.getField(2, StringValue.class);
        String text = tweet.getValue();

        if (text != null) {
            isSpam.setValue(0);

            // Multiple else if statements for debug purposes
            if(spam_earn.matcher(text).matches()){
                isSpam.setValue(1);
            } else if(spam_watch.matcher(text).matches()){
                isSpam.setValue(2);
            } else if(spam_download.matcher(text).matches()){
                isSpam.setValue(3);
            } else if(spam_win.matcher(text).matches()){
                isSpam.setValue(4);
            } else if(spam_free.matcher(text).matches()){
                isSpam.setValue(5);
            }

            if(isSpam.getValue() > 0){
                pr2.setField(0, pr.getField(0, LongValue.class));
                pr2.setField(1, pr.getField(1, LongValue.class));
                pr2.setField(2, isSpam);
                records.collect(pr2);
                if(TweetCleanse.SentimentAnalysisMapLog){
                  //System.out.printf("SAM out %d \n", pr2.getField(0, LongValue.class).getValue() );
                  this.counter++;
                }
            }

        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.SpamFlagMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }


}
