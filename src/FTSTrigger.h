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

#include <list>
#include <map>
#include <string>
#include <string_view>
#include <unordered_set>

#include "LuceneUdr.h"

#include "FTSIndex.h"

namespace FTSMetadata
{

    class FTSKeyFieldBlock final
    {
    public:
        std::string keyFieldName;
        FTSKeyType keyFieldType{ FTSKeyType::NONE };
        std::unordered_set<std::string> fieldNames;

        std::string insertingCondition;
        std::string updatingCondition;
        std::string deletingCondition;
    public:
        FTSKeyFieldBlock() = default;

        FTSKeyFieldBlock(const std::string& aKeyFieldName, FTSKeyType aKeyFieldType);

        // non-copyable
        FTSKeyFieldBlock(const FTSKeyFieldBlock& rhs) = delete;
        FTSKeyFieldBlock& operator=(const FTSKeyFieldBlock& rhs) = delete;
        // movable
        FTSKeyFieldBlock(FTSKeyFieldBlock&& rhs) noexcept = default;
        FTSKeyFieldBlock& operator=(FTSKeyFieldBlock&& rhs) noexcept = default;

        std::string makeInsertSQL(const std::string& relationName, char opType, unsigned int sqlDialect) const;
    };

    using FTSKeyFieldBlockMap = std::map<std::string, FTSKeyFieldBlock>;

    class FTSTrigger final
    {
    public:
        std::string triggerName;
        std::string relationName;
        std::string triggerEvents;
        short position = 0;
        std::string triggerSource;
    public:
        FTSTrigger() = default;

        FTSTrigger(
            const std::string& aTriggerName, 
            const std::string& aRelationName, 
            std::string_view aTriggerEvents, 
            short aPosition,
            const std::string& aTriggerSource
        )
            : triggerName(aTriggerName)
            , relationName(aRelationName)
            , triggerEvents(aTriggerEvents)
            , position(aPosition)
            , triggerSource(aTriggerSource)
        {}

        // non-copyable
        FTSTrigger(const FTSTrigger& rhs) = delete;
        FTSTrigger& operator=(const FTSTrigger& rhs) = delete;
        // movable
        FTSTrigger(FTSTrigger&& rhs) noexcept = default;
        FTSTrigger& operator=(FTSTrigger&& rhs) noexcept = default;

        std::string getHeader(unsigned int sqlDialect) const;

        std::string getScript(unsigned int sqlDialect) const;
    };

    using FTSTriggerList = std::list<FTSTrigger>;

    class FTSTriggerHelper final
    {

    private:
        Firebird::IMaster* m_master = nullptr;

    public:
        FTSTriggerHelper() = delete;

        explicit FTSTriggerHelper(Firebird::IMaster* master);

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
        /// 
        FTSTriggerList makeTriggerSourceByRelation(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& relationName,
            bool multiAction,
            short position
        );

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
        /// 
        FTSKeyFieldBlockMap fillKeyFieldBlocks(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        std::string makeTriggerSourceByRelationMulti(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        std::string makeTriggerSourceByRelationInsert(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        std::string makeTriggerSourceByRelationUpdate(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );

        std::string makeTriggerSourceByRelationDelete(
            const FTSKeyFieldBlockMap& keyFieldBlocks,
            unsigned int sqlDialect,
            const std::string& relationName
        );
    };
}

#endif
