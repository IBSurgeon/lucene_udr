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
#include "EncodeUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "lucene++/QueryScorer.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;


/***
PROCEDURE FTS$MAKE_TRIGGER (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$MULTI_ACTION BOOLEAN DEFAULT TRUE
)
RETURNS (
	FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsMakeTrigger'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsMakeTrigger)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_BOOLEAN, multi_action)
	);

	FB_UDR_MESSAGE(OutMessage,
		(FB_BLOB, triggerSource)
	);

	FB_UDR_CONSTRUCTOR
	, indexRepository(context->getMaster())
	{
	}

	FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$RELATION_NAME can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string relationName(in->relation_name.str, in->relation_name.length);

		bool multiActionFlag = true;
		if (!in->multi_actionNull) {
			multiActionFlag = in->multi_action;
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);


		try {
			// TODO: needs map source of trigger events
			triggerSources = procedure->indexRepository.makeTriggerSourceByRelation(status, att, tra, sqlDialect, relationName, multiActionFlag);
			it = triggerSources.begin();
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	list<string> triggerSources;
	list<string>::iterator it;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;


	FB_UDR_FETCH_PROCEDURE
	{
		if (it == triggerSources.end()) {
			out->triggerSourceNull = true;
			return false;
		}


		string triggerSource = *it;

		out->triggerSourceNull = false;
		AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->triggerSource, 0, nullptr));
		blob_set_string(status, blob, triggerSource);
		blob->close(status);

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
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)error_message.c_str(),
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
			}
		}
		if (action == IExternalTrigger::ACTION_UPDATE) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
			string hexDbKey = string_to_hex(dbKey);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)hexDbKey.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
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
