#ifndef LUCENE_ENGLISH_ANALYZER_H
#define LUCENE_ENGLISH_ANALYZER_H

/**
 *  Analyzer for English.
 *
 *  The original code was created by Simonov Denis
 *  for the open source project "IBSurgeon Full Text Search UDR".
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/


#include "Analyzer.h"

namespace Lucene {
	class EnglishAnalyzer : public Analyzer {
	public:
        /// Builds an analyzer with the default stop words ({@link #STOP_WORDS_SET}).
        /// @param matchVersion Lucene version to match.
        EnglishAnalyzer(LuceneVersion::Version matchVersion);

        /// Builds an analyzer with the given stop words.
        /// @param matchVersion Lucene version to match.
        /// @param stopWords stop words
        EnglishAnalyzer(LuceneVersion::Version matchVersion, HashSet<String> stopWords);

        /// Builds an analyzer with the stop words from the given file.
        /// @see WordlistLoader#getWordSet(const String&, const String&)
        /// @param matchVersion Lucene version to match.
        /// @param stopwords File to read stop words from.
        EnglishAnalyzer(LuceneVersion::Version matchVersion, const String& stopwords);

        /// Builds an analyzer with the stop words from the given reader.
        /// @see WordlistLoader#getWordSet(ReaderPtr, const String&)
        /// @param matchVersion Lucene version to match.
        /// @param stopwords Reader to read stop words from.
        EnglishAnalyzer(LuceneVersion::Version matchVersion, const ReaderPtr& stopwords);

        virtual ~EnglishAnalyzer();

		LUCENE_CLASS(EnglishAnalyzer);

    public:
        /// Default maximum allowed token length
        static const int32_t DEFAULT_MAX_TOKEN_LENGTH;

    protected:
        HashSet<String> stopSet;

        /// Specifies whether deprecated acronyms should be replaced with HOST type.
        bool replaceInvalidAcronym;
        bool enableStopPositionIncrements;

        LuceneVersion::Version matchVersion;

        int32_t maxTokenLength;

    protected:
        /// Construct an analyzer with the given stop words.
        void ConstructAnalyser(LuceneVersion::Version matchVersion, HashSet<String> stopWords);

	public:
        /// Returns an unmodifiable instance of the default stop-words set.
        static const HashSet<String> getDefaultStopSet();

		/// Constructs a {@link StandardTokenizer} filtered by a {@link StandardFilter}, a {@link LowerCaseFilter}
		/// a {@link StopFilter} and a {@link PorterStemFilter}.
		TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        /// Set maximum allowed token length.  If a token is seen that exceeds this length then it is discarded.  This setting
        /// only takes effect the next time tokenStream or reusableTokenStream is called.
        void setMaxTokenLength(int32_t length);

        /// @see #setMaxTokenLength
        int32_t getMaxTokenLength();

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class EnglishAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~EnglishAnalyzerSavedStreams();

    public:
        StandardTokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
