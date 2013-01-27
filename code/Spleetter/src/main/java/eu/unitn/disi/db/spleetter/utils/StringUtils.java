/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.unitn.disi.db.spleetter.utils;

import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utility class for efficient string operations on strings containing ASCII characters only. The operations are more
 * efficient, because they use a very simple encoding logic and operate on mutable objects, sparing object allocation
 * and garbage collection overhead.
 */
public class StringUtils {
        public static String[] words = {
        "a",
        "about",
        "above",
        "according",
        "across",
        "after",
        "afterwards",
        "again",
        "against",
        "albeit",
        "all",
        "almost",
        "alone",
        "along",
        "already",
        "also",
        "although",
        "always",
        "among",
        "amongst",
        "am",
        "an",
        "and",
        "another",
        "any",
        "anybody",
        "anyhow",
        "anyone",
        "anything",
        "anyway",
        "anywhere",
        "apart",
        "are",
        "around",
        "as",
        "at",
        "av",
        "be",
        "became",
        "because",
        "become",
        "becomes",
        "becoming",
        "been",
        "before",
        "beforehand",
        "behind",
        "being",
        "below",
        "beside",
        "besides",
        "between",
        "beyond",
        "both",
        "but",
        "by",
        "can",
        "cannot",
        "canst",
        "certain",
        "cf",
        "choose",
        "contrariwise",
        "cos",
        "could",
        "cu",
        "day",
        "do",
        "does",
        "doesn't",
        "doing",
        "dost",
        "double",
        "down",
        "dual",
        "during",
        "each",
        "either",
        "else",
        "elsewhere",
        "enough",
        "et",
        "etc",
        "even",
        "ever",
        "every",
        "everybody",
        "everyone",
        "everything",
        "everywhere",
        "except",
        "excepted",
        "xcepting",
        "exception",
        "exclude",
        "excluding",
        "exclusive",
        "far",
        "farther",
        "farthest",
        "few",
        "ff",
        "first",
        "for",
        "former",
        "formerly",
        "forth",
        "forward",
        "from",
        "front",
        "further",
        "furthermore",
        "furthest",
        "get",
        "go",
        "had",
        "halves",
        "haedly",
        "has",
        "hast",
        "hath",
        "have",
        "he",
        "hence",
        "henceforth",
        "her",
        "here",
        "hereabouts",
        "hereafter",
        "hereby",
        "herein",
        "hereto",
        "hereupon",
        "hers",
        "herself",
        "him",
        "himself",
        "hindmost",
        "his",
        "hither",
        "how",
        "however",
        "howsoever",
        "i",
        "ie",
        "if",
        "in",
        "inasmuch",
        "inc",
        "include",
        "included",
        "including",
        "indeed",
        "indoors",
        "inside",
        "insomuch",
        "instead",
        "into",
        "inward",
        "is",
        "it",
        "its",
        "itself",
        "just",
        "kind",
        "kg",
        "km",
        "last",
        "latter",
        "latterly",
        "less",
        "lest",
        "let",
        "like",
        "little",
        "ltd",
        "many",
        "may",
        "maybe",
        "me",
        "meantime",
        "meanwhile",
        "might",
        "moreover",
        "most",
        "mostly",
        "more",
        "mr",
        "mrs",
        "ms",
        "much",
        "must",
        "my",
        "myself",
        "namely",
        "need",
        "neither",
        "never",
        "nevertheless",
        "next",
        "no",
        "nobody",
        "none",
        "nonetheless",
        "noone",
        "nope",
        "nor",
        "not",
        "nothing",
        "notwithstanding",
        "now",
        "nowadays",
        "nowhere",
        "of",
        "off",
        "often",
        "ok",
        "on",
        "once one",
        "only",
        "onto",
        "or",
        "other",
        "others",
        "otherwise",
        "ought",
        "our",
        "ours",
        "ourselves",
        "out",
        "outside",
        "over",
        "own",
        "per",
        "perhaps",
        "plenty",
        "provide",
        "quite",
        "rather",
        "really",
        "round",
        "s",
        "said",
        "same",
        "sang",
        "save",
        "saw",
        "see",
        "seeing",
        "seem",
        "seemed",
        "seeming",
        "seems",
        "seen",
        "seldom",
        "selves",
        "sent",
        "several",
        "shalt",
        "she",
        "should",
        "shown",
        "sideways",
        "since",
        "slept",
        "slew",
        "slung",
        "slunk",
        "smote",
        "so",
        "some",
        "somebody",
        "somehow",
        "someone",
        "something",
        "sometime",
        "sometimes",
        "somewhat",
        "somewhere",
        "spake",
        "spat",
        "spoke",
        "spoken",
        "sprang",
        "sprung",
        "staves",
        "still",
        "such",
        "supposing",
        "t",
        "than",
        "that",
        "the",
        "thee",
        "their",
        "them",
        "themselves",
        "then",
        "thence",
        "thenceforth",
        "there",
        "thereabout",
        "thereabouts",
        "thereafter",
        "thereby",
        "therefore",
        "therein",
        "thereof",
        "thereon",
        "thereto",
        "thereupon",
        "these",
        "they",
        "this",
        "those",
        "thou",
        "though",
        "thrice",
        "through",
        "throughout",
        "thru",
        "thus",
        "thy",
        "thyself",
        "till",
        "to",
        "together",
        "too",
        "toward",
        "towards",
        "ugh",
        "unable",
        "under",
        "underneath",
        "unless",
        "unlike",
        "until",
        "up",
        "upon",
        "upward",
        "us",
        "use",
        "used",
        "using",
        "very",
        "via",
        "vs",
        "want",
        "was",
        "we",
        "week",
        "well",
        "were",
        "what",
        "whatever",
        "whatsoever",
        "when",
        "whence",
        "whenever",
        "whensoever",
        "where",
        "whereabouts",
        "whereafter",
        "whereas",
        "whereat",
        "whereby",
        "wherefore",
        "wherefrom",
        "wherein",
        "whereinto",
        "whereof",
        "whereon",
        "wheresoever",
        "whereto",
        "whereunto",
        "whereupon",
        "wherever",
        "wherewith",
        "whether",
        "whew",
        "which",
        "whichever",
        "whichsoever",
        "while",
        "whilst",
        "whither",
        "who",
        "whoever",
        "whole",
        "whom",
        "whomever",
        "whomsoever",
        "whose",
        "whosoever",
        "why",
        "will",
        "wilt",
        "with",
        "within",
        "without",
        "worse",
        "worst",
        "would",
        "wow",
        "ye",
        "yet",
        "year",
        "yipee",
        "you",
        "your",
        "yours",
        "yourself",
        "yourselves"
    };


    /**
     * Converts the given <code>PactString</code> into a lower case variant.
     * <p>
     * NOTE: This method assumes that the string contains only characters that are valid in the
     * ASCII type set.
     *
     * @param string The string to convert to lower case.
     */
    public static void toLowerCase(PactString string)
    {
        final char[] chars = string.getCharArray();
        final int len = string.length();

        for (int i = 0; i < len; i++) {
                chars[i] = Character.toLowerCase(chars[i]);
        }
    }

    /**
     * Replaces all non-word characters in a string by a given character. The only
     * characters not replaced are <code>A-Z, a-z, 0-9, and _</code>.
     * <p>
     * This operation is intended to simplify strings for counting distinct words.
     *
     * @param string The pact string to have the non-word characters replaced.
     * @param replacement The character to use as the replacement.
     */
    public static void replaceNonWordChars(PactString string, char replacement) {
        final char[] chars = string.getCharArray();

        replaceNonWordChars(chars, replacement);
    }

    public static void replaceNonWordChars(char[] chars, char replacement) {
        final int len = chars.length;

        for (int i = 0; i < len; i++) {
            final char c = chars[i];
            if (!(Character.isLetter(c) || Character.isDigit(c) || c == '_')) {
                chars[i] = replacement;
            }
        }
    }


    public synchronized static Set<String> getEnglishDictionary(String file) throws Exception {
        Set<String> englishDictionary = null;
        englishDictionary = new HashSet<String>();
        BufferedReader reader = null;
        String line = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            while ((line = reader.readLine()) != null) {
                englishDictionary.add(line.toLowerCase());
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {}
            }
        }
        return englishDictionary;
    }

    private static final Pattern wordSepPattern = Pattern.compile("([^\\p{L}^\\p{Digit}]+)");
    private static Set<String> stopWords = null;


    public static int numWords(String line) {
        if (line == null || line.length() == 0)
            return 0;
        return wordSepPattern.split(line).length;
    }

    public static String removeStopwords(String sentence) {
        String[] splittedSentence = wordSepPattern.split(sentence);
        StringBuilder sb = new StringBuilder();
        if (stopWords == null) {
            stopWords = new HashSet<String>();
            for (int i = 0; i < words.length; i++) {
                stopWords.add(words[i]);
            }
        }
        for (int i = 0; i < splittedSentence.length; i++) {
            if (!stopWords.contains(splittedSentence[i]))
                sb.append(splittedSentence[i]).append(" ");
        }
        return sb.toString();
    }

    // ============================================================================================
    /**
     * A tokenizer for pact strings that uses whitespace characters as token delimiters.
     * The tokenizer is designed to have a resettable state and operate on mutable objects,
     * sparing object allocation and garbage collection overhead.
     */
    public static final class WhitespaceTokenizer implements MutableObjectIterator<PactString> {

        private PactString toTokenize;		// the string to tokenize
        private int pos;					// the current position in the string
        private int limit;					// the limit in the string's character data

        /**
         * Creates a new tokenizer with an undefined internal state.
         */
        public WhitespaceTokenizer() {
        }

        /**
         * Sets the string to be tokenized and resets the state of the tokenizer.
         *
         * @param string The pact string to be tokenized.
         */
        public void setStringToTokenize(PactString string) {
            this.toTokenize = string;
            this.pos = 0;
            this.limit = string.length();
        }

        /**
         * Gets the next token from the string. If another token is available, the token is stored
         * in the given target string object and <code>true</code> is returned. Otherwise,
         * the target object is left unchanged and <code>false</code> is returned.
         *
         * @param target The PactString object to store the next token in.
         * @return True, if there was another token, false if not.
         * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
         */
        @Override
        public boolean next(PactString target) {
            final char[] data = this.toTokenize.getCharArray();
            final int limit = this.limit;
            int pos = this.pos;

            // skip the delimiter
            for (; pos < limit && Character.isWhitespace(data[pos]); pos++);

            if (pos >= limit) {
                this.pos = pos;
                return false;
            }

            final int start = pos;
            for (; pos < limit && !Character.isWhitespace(data[pos]); pos++);
            this.pos = pos;
            target.setValue(this.toTokenize, start, pos - start);
            return true;
        }
    }

    // ============================================================================================
    /**
     * Private constructor to prevent instantiation, as this is a utility method encapsulating class.
     */
    private StringUtils() {
    }
}