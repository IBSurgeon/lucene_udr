/**
 *  Implementation of procedures and functions of the FTS$TRIGGER_HELPER package.
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

#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "FBUtils.h"
#include "LuceneHeaders.h"
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;


/***
PROCEDURE FTS$MAKE_TRIGGER (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$MULTI_ACTION BOOLEAN DEFAULT TRUE NOT NULL,
	FTS$POSITION SMALLINT DEFAULT 100 NOT NULL
)
RETURNS (
    FTS$TRIGGER_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_RELATION VARCHAR(63) CHARACTER SET UTF8,
	FTS$TRIGGER_EVENTS VARCHAR(26) CHARACTER SET UTF8,
	FTS$TRIGGER_POSITION SMALLINT,
	FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
	FTS$TRIGGER_SCRIPT BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsMakeTrigger'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsMakeTrigger)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_BOOLEAN, multiAction)
		(FB_SMALLINT, position)
	);

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), triggerName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(104, CS_UTF8), events)
		(FB_SMALLINT, position)
		(FB_BLOB, triggerSource)
		(FB_BLOB, triggerScript)
	);

	FB_UDR_CONSTRUCTOR
	, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
		char* name, unsigned nameSize)
	{
		// Forced internal request encoding to UTF8
		memset(name, 0, nameSize);

		const string charset = "UTF8";
		charset.copy(name, charset.length());
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relationNameNull) {
			throwException(status, "FTS$RELATION_NAME can not be NULL");
		}
		string relationName(in->relationName.str, in->relationName.length);

		const bool multiActionFlag = in->multiAction;

		const auto triggerPosition = in->position;

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		sqlDialect = getSqlDialect(status, att);


		try {
			procedure->indexRepository->makeTriggerSourceByRelation(status, att, tra, sqlDialect, relationName, multiActionFlag, triggerPosition, triggers);
			it = triggers.cbegin();
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	FTSTriggerList triggers;
	FTSTriggerList::const_iterator it;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	unsigned int sqlDialect;


	FB_UDR_FETCH_PROCEDURE
	{
		if (it == triggers.end()) {
			return false;
		}

		const auto& trigger = *it;

		out->triggerNameNull = false;
		out->triggerName.length = static_cast<ISC_SHORT>(trigger->triggerName.length());
		trigger->triggerName.copy(out->triggerName.str, out->triggerName.length);

		out->relationNameNull = false;
		out->relationName.length = static_cast<ISC_SHORT>(trigger->relationName.length());
		trigger->relationName.copy(out->relationName.str, out->relationName.length);

		out->eventsNull = false;
		out->events.length = static_cast<ISC_SHORT>(trigger->triggerEvents.length());
		trigger->triggerEvents.copy(out->events.str, out->events.length);

		out->positionNull = false;
		out->position = trigger->position;

		out->triggerSourceNull = false;
		{
			AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->triggerSource, 0, nullptr));
			BlobUtils::setString(status, blob, trigger->triggerSource);
			blob->close(status);
		}

		out->triggerScriptNull = false;
		{
			AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->triggerScript, 0, nullptr));
			BlobUtils::setString(status, blob, trigger->getScript(sqlDialect));
			blob->close(status);
		}

		++it;
		return true;
	}
FB_UDR_END_PROCEDURE



/*

// CREATE OR ALTER TRIGGER FTS$TR_HORSE FOR HORSE
// ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100
// EXTERNAL NAME 'luceneudr!trFtsLog'
// ENGINE UDR;

FB_UDR_BEGIN_TRIGGER(trFtsLog)

	FB_UDR_CONSTRUCTOR
	   , triggerTable(metadata->getTriggerTable(status))
	   , indexRepository(context->getMaster())
	{

		AutoRelease<IMessageMetadata> origTriggerMetadata(metadata->getTriggerMetadata(status));
		AutoRelease<IMetadataBuilder> builder(origTriggerMetadata->getBuilder(status));
		auto fieldIndex = builder->addField(status);
		builder->setField(status, fieldIndex, "RDB$DB_KEY");
		builder->setType(status, fieldIndex, SQL_TEXT);
		builder->setLength(status, fieldIndex, 8);
		builder->setCharSet(status, fieldIndex, CS_BINARY);
		triggerMetadata.reset(builder->getMetadata(status));
	}

	FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_TRIGGER
	{
		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// получаем сегменты FTS индекса
		auto segments = indexRepository.getIndexSegmentsByRelation(status, att, tra, sqlDialect, triggerTable);
		// если нет ни одного сегмента выходим
		if (segments.size() == 0)
			return;
		auto fieldsInfo = getFieldsInfo(status, triggerMetadata);
		int dbKeyIndex = findFieldByName(fieldsInfo, "RDB$DB_KEY");


		if (action == IExternalTrigger::ACTION_INSERT) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
			bool changeFlag = false;
			for (auto& segment : segments) {
				int fieldIndex = findFieldByName(fieldsInfo, segment.fieldName);
				if (fieldIndex < 0) {
					string error_message = string_format("Invalid index segment \"%s\".\"%s\" for index \"%s\".", segment.relationName, segment.fieldName, segment.indexName);
					throwException(status, error_message.c_str());
				}
			}
		}
		if (action == IExternalTrigger::ACTION_UPDATE) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
			string hexDbKey = string_to_hex(dbKey);
			throwException(status, hexDbKey.c_str());
		}
		if (action == IExternalTrigger::ACTION_DELETE) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
		}
	}

	string triggerTable;
	AutoRelease<IMessageMetadata> triggerMetadata;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
FB_UDR_END_TRIGGER
*/
