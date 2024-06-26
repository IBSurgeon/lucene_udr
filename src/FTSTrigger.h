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

namespace FTSMetadata
{

    class FTSKeyFieldBlock final
    {
    public:
        std::string keyFieldName{ "" };
        FTSKeyType keyFieldType{ FTSKeyType::NONE };
        std::unordered_set<std::string> fieldNames;

        std::string insertingCondition{ "" };
        std::string updatingCondition{ "" };
        std::string deletingCondition{ "" };
    public:
        FTSKeyFieldBlock() = default;

        std::string getProcedureName() const
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

    using FTSKeyFieldBlockPtr = std::unique_ptr<FTSKeyFieldBlock>;
    using FTSKeyFieldBlockMap = std::map<std::string, FTSKeyFieldBlockPtr>;

    class FTSTrigger final
    {
    public:
        std::string triggerName{ "" };
        std::string relationName{ "" };
        std::string triggerEvents{ "" };
        short position = 0;
        std::string triggerSource{ "" };
    public:
        FTSTrigger() = default;

        FTSTrigger(const std::string& aTriggerName, 
            const std::string& aRelationName, 
            const std::string& aTriggerEvents, 
            short aPosition,
            const std::string& aTriggerSource
        )
            : triggerName(aTriggerName)
            , relationName(aRelationName)
            , triggerEvents(aTriggerEvents)
            , position(aPosition)
            , triggerSource(aTriggerSource)
        {}

        const std::string getHeader(unsigned int sqlDialect);

        const std::string getScript(unsigned int sqlDialect);
    };

    using FTSTriggerPtr = std::unique_ptr<FTSTrigger>;
    using FTSTriggerList = std::list<FTSTriggerPtr>;

    class FTSTriggerHelper final
    {

    private:
        Firebird::IMaster* m_master = nullptr;

    public:
        FTSTriggerHelper() = delete;

        explicit FTSTriggerHelper(Firebird::IMaster* const master);

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
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            const std::string& relationName,
            bool multiAction,
            short position,
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
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            const std::string& relationName,
            FTSKeyFieldBlockMap& keyFieldBlocks
        );

        const std::string makeTriggerSourceByRelationMulti(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        const std::string makeTriggerSourceByRelationInsert(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        const std::string makeTriggerSourceByRelationUpdate(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        const std::string makeTriggerSourceByRelationDelete(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );
    };
}

#endif
