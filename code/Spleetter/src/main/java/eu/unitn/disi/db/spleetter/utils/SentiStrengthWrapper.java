/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.utils;

import java.io.Serializable;
import uk.ac.wlv.sentistrength.SentiStrength;

/**
 * Wrapper for the SentiStrength library
 *
 */
public class SentiStrengthWrapper implements Serializable {

    private SentiStrength classifier;

    /** The unique instance **/
    private volatile static SentiStrengthWrapper instance;

    /** The private constructor **/
    private SentiStrengthWrapper() {
        String[] args = {"sentidata", "/tmp/SentiStrength_Data/", "text", "i don't hate you. I really hate you"};
        classifier = new SentiStrength(args);
    }

    public static SentiStrengthWrapper getInstance() {
      if (instance == null) {
          synchronized(SentiStrengthWrapper.class) {
             if (instance == null) {
                instance = new SentiStrengthWrapper();
             }
          }
       }
       return instance;
    }


    public double[] analyze(String text){
        double[] polarities = new double[2];

        String result = this.classifier.computeSentimentScores(text);
        String [] tokens = result.split(" ");
        polarities[0] = Double.parseDouble(tokens[1]);
        polarities[1] = Double.parseDouble(tokens[0]);
        return polarities;
    }
}
