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

#include "LuceneHeaders.h"
#include "WordlistLoader.h"
#include "StandardAnalyzer.h"
#include "EnglishAnalyzer.h"
#include "PorterStemFilter.h"

namespace Lucene {

    /// Construct an analyzer with the given stop words.
    const int32_t EnglishAnalyzer::DEFAULT_MAX_TOKEN_LENGTH = 255;

    EnglishAnalyzer::EnglishAnalyzer(LuceneVersion::Version matchVersion) {
        ConstructAnalyser(matchVersion, StopAnalyzer::ENGLISH_STOP_WORDS_SET());
    }

    EnglishAnalyzer::EnglishAnalyzer(LuceneVersion::Version matchVersion, HashSet<String> stopWords) {
        ConstructAnalyser(matchVersion, stopWords);
    }

    EnglishAnalyzer::EnglishAnalyzer(LuceneVersion::Version matchVersion, const String& stopwords) {
        ConstructAnalyser(matchVersion, WordlistLoader::getWordSet(stopwords));
    }

    EnglishAnalyzer::EnglishAnalyzer(LuceneVersion::Version matchVersion, const ReaderPtr& stopwords) {
        ConstructAnalyser(matchVersion, WordlistLoader::getWordSet(stopwords));
    }

    EnglishAnalyzer::~EnglishAnalyzer() {
    }

    void EnglishAnalyzer::ConstructAnalyser(LuceneVersion::Version matchVersion, HashSet<String> stopWords) {
        stopSet = stopWords;
        enableStopPositionIncrements = StopFilter::getEnablePositionIncrementsVersionDefault(matchVersion);
        replaceInvalidAcronym = LuceneVersion::onOrAfter(matchVersion, LuceneVersion::LUCENE_24);
        this->matchVersion = matchVersion;
        this->maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;
    }

    /// Returns an unmodifiable instance of the default stop-words set.
    const HashSet<String> EnglishAnalyzer::getDefaultStopSet()
    {
        return StopAnalyzer::ENGLISH_STOP_WORDS_SET();
    }

    /// Constructs a {@link StandardTokenizer} filtered by a {@link StandardFilter}, a {@link LowerCaseFilter}
    /// a {@link StopFilter} and a {@link PorterStemFilter}.
    TokenStreamPtr EnglishAnalyzer::tokenStream(const String& fieldName, const ReaderPtr& reader)
    {
        StandardTokenizerPtr tokenStream(newLucene<StandardTokenizer>(matchVersion, reader));
        tokenStream->setMaxTokenLength(maxTokenLength);
        TokenStreamPtr result(newLucene<StandardFilter>(tokenStream));
        result = newLucene<LowerCaseFilter>(result);
        result = newLucene<StopFilter>(enableStopPositionIncrements, result, stopSet);
        result = newLucene<PorterStemFilter>(result);
        return result;
    }

    void EnglishAnalyzer::setMaxTokenLength(int32_t length) {
        maxTokenLength = length;
    }

    int32_t EnglishAnalyzer::getMaxTokenLength() {
        return maxTokenLength;
    }

    TokenStreamPtr EnglishAnalyzer::reusableTokenStream(const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<EnglishAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<EnglishAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<StandardTokenizer>(matchVersion, reader);
            streams->filteredTokenStream = newLucene<StandardFilter>(streams->tokenStream);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->filteredTokenStream);
            streams->filteredTokenStream = newLucene<StopFilter>(enableStopPositionIncrements, streams->filteredTokenStream, stopSet);
            streams->filteredTokenStream = newLucene<PorterStemFilter>(streams->filteredTokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        streams->tokenStream->setMaxTokenLength(maxTokenLength);

        streams->tokenStream->setReplaceInvalidAcronym(replaceInvalidAcronym);

        return streams->filteredTokenStream;
    }

    EnglishAnalyzerSavedStreams::~EnglishAnalyzerSavedStreams() {
    }

}