#ifndef FTS_TRIGGER_H
#define FTS_TRIGGER_H

/**
 *  Utilities for building FTS triggers for full-text indexes.
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

#include "LuceneUdr.h"
#include "FTSIndex.h"
#include <string>
#include <list>
#include <map>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace std;


namespace FTSMetadata
{

	class FTSKeyFieldBlock final
	{
	public:
		string keyFieldName{ "" };
		FTSKeyType keyFieldType{ FTSKeyType::NONE };
		unordered_set<string> fieldNames;

		string insertingCondition{ "" };
		string updatingCondition{ "" };
		string deletingCondition{ "" };
	public:
		FTSKeyFieldBlock() = default;

		const string getProcedureName()
		{
			switch (keyFieldType) {
			case FTSKeyType::DB_KEY:
				return "FTS$LOG_BY_DBKEY";
			case FTSKeyType::INT_ID:
				return "FTS$LOG_BY_ID";
			case FTSKeyType::UUID:
				return "FTS$LOG_BY_UUID";
			default:
				return "";
			}
		}
	};

	using FTSKeyFieldBlockPtr = unique_ptr<FTSKeyFieldBlock>;
	using FTSKeyFieldBlockMap = map<string, FTSKeyFieldBlockPtr>;

	class FTSTrigger final
	{
	public:
		string triggerName{ "" };
		string relationName{ "" };
		string triggerEvents{ "" };
		unsigned int position = 0;
		string triggerSource{ "" };
	public:
		FTSTrigger() = default;

		FTSTrigger(const string& aTriggerName, const string& aRelationName, const string& aTriggerEvents, const unsigned int aPosition, const string& aTriggerSource)
			: triggerName(aTriggerName)
			, relationName(aRelationName)
			, triggerEvents(aTriggerEvents)
			, position(aPosition)
			, triggerSource(aTriggerSource)
		{}

		const string getHeader(unsigned int sqlDialect);

		const string getScript(unsigned int sqlDialect);
	};

	using FTSTriggerPtr = unique_ptr<FTSTrigger>;
	using FTSTriggerList = list<FTSTriggerPtr>;

	class FTSTriggerHelper final
	{

	private:
		IMaster* m_master = nullptr;

	public:
		FTSTriggerHelper() = delete;

		explicit FTSTriggerHelper(IMaster* const master);

		~FTSTriggerHelper();

		/// <summary>
		/// Returns a list of trigger source codes to support full-text indexes by relation name. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="multiAction">Flag for generating multi-event triggers</param>
		/// <param name="position">Trigger position</param>
		/// <param name="triggers">Triggers list</param>
		/// 
		void makeTriggerSourceByRelation(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const bool multiAction,
			const unsigned short position,
			FTSTriggerList& triggers);

	private:
		/// <summary>
		/// Returns a map of field blocks by table keys to create triggers that support full-text indexes.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="keyFieldBlocks">Map of field blocks by table keys</param>
		/// 
		void fillKeyFieldBlocks(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			FTSKeyFieldBlockMap& keyFieldBlocks
		);

		const string makeTriggerSourceByRelationMulti(
			const FTSKeyFieldBlockMap& keyFieldBlocks,
			const unsigned int sqlDialect,
			const string& relationName
		);

		const string makeTriggerSourceByRelationInsert(
			const FTSKeyFieldBlockMap& keyFieldBlocks,
			const unsigned int sqlDialect,
			const string& relationName
		);

		const string makeTriggerSourceByRelationUpdate(
			const FTSKeyFieldBlockMap& keyFieldBlocks,
			const unsigned int sqlDialect,
			const string& relationName
		);

		const string makeTriggerSourceByRelationDelete(
			const FTSKeyFieldBlockMap& keyFieldBlocks,
			const unsigned int sqlDialect,
			const string& relationName
		);
	};
}

#endif
