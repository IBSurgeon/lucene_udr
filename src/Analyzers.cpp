/**
 *  Utilities for getting and managing metadata for analyzers.
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

#include "Analyzers.h"
#include "LuceneAnalyzerFactory.h"
#include "FBUtils.h"
#include "Utils.h"

using namespace LuceneUDR;

namespace FTSMetadata
{

	AnalyzerRepository::AnalyzerRepository(IMaster* const master)
		: m_master(master)
		, m_analyzerFactory(make_unique<LuceneAnalyzerFactory>())
	{}

	AnalyzerRepository::~AnalyzerRepository() = default;

	AnalyzerPtr AnalyzerRepository::createAnalyzer(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			return m_analyzerFactory->createAnalyzer(status, analyzerName);
		}
		const auto& info = getAnalyzerInfo(status, att, tra, sqlDialect, analyzerName);
		if (!m_analyzerFactory->hasAnalyzer(info.baseAnalyzer)) {
			throwException(status, R"(Base analyzer "%s" not exists)", info.baseAnalyzer.c_str());
		}
		const auto stopWords = getStopWords(status, att, tra, sqlDialect, analyzerName);
		return m_analyzerFactory->createAnalyzer(status, info.baseAnalyzer, stopWords);
	}

	const AnalyzerInfo AnalyzerRepository::getAnalyzerInfo(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			return m_analyzerFactory->getAnalyzerInfo(status, analyzerName);
		}
		AnalyzerInfo info;

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
			(FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
			(FB_BLOB, description)
		) output(status, m_master);

		input.clear();
		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		if (!m_stmt_get_analyzer.hasData()) {
			m_stmt_get_analyzer.reset(att->prepare(
				status,
				tra,
				0,
				SQL_ANALYZER_INFO,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_get_analyzer->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));

		int result = rs->fetchNext(status, output.getData());
		if (result == IStatus::RESULT_NO_DATA) {
			throwException(status, R"(Analyzer "%s" not exists)", analyzerName.c_str());
		}
		rs->close(status);

		if (result == IStatus::RESULT_OK) {
			info.analyzerName.assign(output->analyzerName.str, output->analyzerName.length);
			info.baseAnalyzer.assign(output->baseAnalyzer.str, output->baseAnalyzer.length);
			info.stopWordsSupported = m_analyzerFactory->isStopWordsSupported(info.baseAnalyzer);
			info.systemFlag = false;
		}
		return info;
	}

	list<AnalyzerInfo> AnalyzerRepository::getAnalyzerInfos(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect
	)
	{
		auto infos = m_analyzerFactory->getAnalyzerInfos();

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
			(FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
			(FB_BLOB, description)
		) output(status, m_master);

		if (!m_stmt_get_analyzers.hasData()) {
			m_stmt_get_analyzers.reset(att->prepare(
				status,
				tra,
				0,
				SQL_ANALYZER_INFOS,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_get_analyzers->openCursor(
			status,
			tra,
			nullptr,
			nullptr,
			outputMetadata,
			0
		));

		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			AnalyzerInfo info;
			info.analyzerName.assign(output->analyzerName.str, output->analyzerName.length);
			info.baseAnalyzer.assign(output->baseAnalyzer.str, output->baseAnalyzer.length);
			info.stopWordsSupported = m_analyzerFactory->isStopWordsSupported(info.baseAnalyzer);
			info.systemFlag = false;
			infos.push_back(info);
		}
		rs->close(status);

		return infos;
	}

	bool AnalyzerRepository::hasAnalyzer (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			return true;
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTEGER, cnt)
		) output(status, m_master);

		input.clear();
		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		if (!m_stmt_has_analyzer.hasData()) {
			m_stmt_has_analyzer.reset(att->prepare(
				status,
				tra,
				0,
				SQL_ANALYZER_EXISTS,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_has_analyzer->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));
		bool foundFlag = false;
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			foundFlag = (output->cnt > 0);
		}
		rs->close(status);

		return foundFlag;
	}

	void AnalyzerRepository::addAnalyzer (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName,
		const string& baseAnalyzer,
		const string& description
	)
	{
		if (hasAnalyzer(status, att, tra, sqlDialect, analyzerName)) {
			throwException(status, R"(Cannot create analyzer. Analyzer "%s" already exists)", analyzerName.c_str());
		}

		if (!m_analyzerFactory->hasAnalyzer(baseAnalyzer)) {
			throwException(status, R"(Cannot create analyzer. Base analyzer "%s" not exists or not system analyzer)", baseAnalyzer.c_str());
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
			(FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
			(FB_BLOB, description)
		) input(status, m_master);

		input.clear();

		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		input->baseAnalyzer.length = static_cast<ISC_USHORT>(baseAnalyzer.length());
		baseAnalyzer.copy(input->baseAnalyzer.str, input->baseAnalyzer.length);

		if (!description.empty()) {
			AutoRelease<IBlob> blob(att->createBlob(status, tra, &input->description, 0, nullptr));
			BlobUtils::setString(status, blob, description);
			blob->close(status);
		}
		else {
			input->descriptionNull = true;
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_INSERT_ANALYZER,
			sqlDialect,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr
		);
	}

	void AnalyzerRepository::deleteAnalyzer(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			throwException(status, R"(Cannot drop system analyzer "%s")", analyzerName.c_str());
		}
		if (!hasAnalyzer(status, att, tra, sqlDialect, analyzerName)) {
			throwException(status, R"(Cannot drop analyzer. Analyzer "%s" not exists)", analyzerName.c_str());
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		) input(status, m_master);

		input.clear();

		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_DELETE_ANALYZER,
			sqlDialect,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr
		);
	}

	const HashSet<String> AnalyzerRepository::getStopWords(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			return m_analyzerFactory->getAnalyzerStopWords(status, analyzerName);
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
		) output(status, m_master);

		input.clear();

		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		auto stopWords = HashSet<String>::newInstance();
		
		if (!m_stmt_get_stopwords.hasData()) {
			m_stmt_get_stopwords.reset(att->prepare(
				status,
				tra,
				0,
				SQL_STOP_WORDS,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		
		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_get_stopwords->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));
		
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			const string stopWord(output->stopWord.str, output->stopWord.length);
			const String uStopWord = StringUtils::toUnicode(stopWord);
			stopWords.add(uStopWord);
		}

		rs->close(status);
		
		return stopWords;
	}

	void AnalyzerRepository::addStopWord(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName,
		const string& stopWord
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			throwException(status, R"(Cannot add stop word to system analyzer "%s")", analyzerName.c_str());
		}
		const auto info = getAnalyzerInfo(status, att, tra, sqlDialect, analyzerName);
		if (!info.stopWordsSupported) {
			throwException(status, R"(Cannot add stop word. Base analyzer "%s" not supported stop words)", info.baseAnalyzer.c_str());
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
			(FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
		) input(status, m_master);

		input.clear();

		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		input->stopWord.length = static_cast<ISC_USHORT>(stopWord.length());
		stopWord.copy(input->stopWord.str, input->stopWord.length);

		if (input->stopWord.length == 0) {
			throwException(status, "Cannot add empty stop word");
		}

		if (!m_stmt_insert_stopword.hasData()) {
			m_stmt_insert_stopword.reset(att->prepare(
				status,
				tra,
				0,
				SQL_INSERT_STOP_WORD,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		m_stmt_insert_stopword->execute(
			status,
			tra,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr);
	}

	void AnalyzerRepository::deleteStopWord(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& analyzerName,
		const string& stopWord
	)
	{
		if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
			throwException(status, R"(Cannot delete stop word from system analyzer "%s")", analyzerName.c_str());
		}
		const auto info = getAnalyzerInfo(status, att, tra, sqlDialect, analyzerName);
		if (!info.stopWordsSupported) {
			throwException(status, R"(Cannot delete stop word. Base analyzer "%s" not supported stop words)", info.baseAnalyzer.c_str());
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
			(FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
		) input(status, m_master);

		input.clear();

		input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

		input->stopWord.length = static_cast<ISC_USHORT>(stopWord.length());
		stopWord.copy(input->stopWord.str, input->stopWord.length);

		if (!m_stmt_delete_stopword.hasData()) {
			m_stmt_delete_stopword.reset(att->prepare(
				status,
				tra,
				0,
				SQL_DELETE_STOP_WORD,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		m_stmt_delete_stopword->execute(
			status,
			tra,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr);
	}

}