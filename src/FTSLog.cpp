/**
 *  Utilities for maintaining the change log.
 *  The journal is used to keep full-text indexes up-to-date.
 *
 *  The original code was created by Simonov Denis
 *  for the open source Lucene UDR full-text search library for Firebird DBMS.
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/

#include "FTSLog.h"

using namespace Firebird;
using namespace std;


namespace LuceneUDR {

	/// <summary>
	/// Adds an entry from the changelog.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="recId">Record ID</param>
	/// <param name="changeType">Type of change</param>
	void FTSLogRepository::appendLogByDbKey(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const string& dbKey,
		const string& changeType)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
			(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
			(FB_BIGINT, recId)
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
		) input(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->dbKey.length = static_cast<ISC_USHORT>(dbKey.length());
		dbKey.copy(input->dbKey.str, input->dbKey.length);

		input->uuidNull = true;

		input->recIdNull = true;

		input->changeType.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->changeType.str, input->changeType.length);

		if (!stmt_append_log.hasData()) {
			stmt_append_log.reset(att->prepare(
				status,
				tra,
				0,
				SQL_APPEND_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		stmt_append_log->execute(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

	/// <summary>
	/// Adds an entry from the changelog.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="dbKey">Record ID</param>
	/// <param name="changeType">Type of change</param>
	void FTSLogRepository::appendLogById(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const ISC_INT64 recId,
		const string& changeType)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
			(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
			(FB_BIGINT, recId)
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
		) input(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->dbKeyNull = true;

		input->uuidNull = true;

		input->recId = recId;

		input->changeType.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->changeType.str, input->changeType.length);

		if (!stmt_append_log.hasData()) {
			stmt_append_log.reset(att->prepare(
				status,
				tra,
				0,
				SQL_APPEND_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		stmt_append_log->execute(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

	/// <summary>
	/// Adds an entry from the changelog.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="dbKey">Record ID</param>
	/// <param name="changeType">Type of change</param>
	void FTSLogRepository::appendLogByUuid(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const string& uuid,
		const string& changeType)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
			(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
			(FB_BIGINT, recId)
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
		) input(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->dbKeyNull = true;

		input->uuid.length = static_cast<ISC_USHORT>(uuid.length());
		uuid.copy(input->uuid.str, input->uuid.length);

		input->recIdNull = true;

		input->changeType.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->changeType.str, input->changeType.length);

		if (!stmt_append_log.hasData()) {
			stmt_append_log.reset(att->prepare(
				status,
				tra,
				0,
				SQL_APPEND_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		stmt_append_log->execute(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

}