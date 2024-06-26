/**
 *  Implementation of procedures and functions of the FTS$TRIGGER_HELPER package.
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
#include "FTSTrigger.h"
#include "FBUtils.h"
#include "LuceneHeaders.h"
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;
using namespace FTSMetadata;


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
        , triggerHelper(std::make_unique<FTSTriggerHelper>(context->getMaster()))
    {
    }

    std::unique_ptr<FTSTriggerHelper> triggerHelper{nullptr};

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        if (in->relationNameNull) {
            throwException(status, "FTS$RELATION_NAME can not be NULL");
        }
        std::string relationName(in->relationName.str, in->relationName.length);

        const bool multiActionFlag = in->multiAction;

        const auto triggerPosition = in->position;

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        sqlDialect = getSqlDialect(status, att);


        try {
            procedure->triggerHelper->makeTriggerSourceByRelation(status, att, tra, sqlDialect, relationName, multiActionFlag, triggerPosition, triggers);
            it = triggers.cbegin();
        }
        catch (LuceneException& e) {
            std::string error_message = StringUtils::toUTF8(e.getError());
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
        out->triggerName.length = static_cast<ISC_USHORT>(trigger->triggerName.length());
        trigger->triggerName.copy(out->triggerName.str, out->triggerName.length);

        out->relationNameNull = false;
        out->relationName.length = static_cast<ISC_USHORT>(trigger->relationName.length());
        trigger->relationName.copy(out->relationName.str, out->relationName.length);

        out->eventsNull = false;
        out->events.length = static_cast<ISC_USHORT>(trigger->triggerEvents.length());
        trigger->triggerEvents.copy(out->events.str, out->events.length);

        out->positionNull = false;
        out->position = trigger->position;

        out->triggerSourceNull = false;
        writeStringToBlob(status, att, tra, &out->triggerSource, trigger->triggerSource);

        out->triggerScriptNull = false;
        writeStringToBlob(status, att, tra, &out->triggerScript, trigger->getScript(sqlDialect));

        ++it;
        return true;
    }
FB_UDR_END_PROCEDURE

