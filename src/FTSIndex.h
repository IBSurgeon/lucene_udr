#ifndef FTS_INDEX_H
#define FTS_INDEX_H

/**
 *  Utilities for getting and managing metadata for full-text indexes.
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
#include <memory>
#include <string>
#include <unordered_set>

#include "LuceneUdr.h"

namespace FTSMetadata
{

    FB_MESSAGE(FTSIndexNameInput, Firebird::ThrowStatusWrapper,
        (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
    );

    FB_MESSAGE(FTSIndexRecord, Firebird::ThrowStatusWrapper,
        (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
        (FB_INTL_VARCHAR(252, CS_UTF8), relationName)
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
        (FB_BLOB, description)
        (FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
    );

    enum class FTSKeyType {NONE, DB_KEY, INT_ID, UUID};

    inline FTSKeyType FTSKeyTypeFromString(std::string_view sKeyFieldType)
    {
        FTSKeyType keyFieldType{ FTSKeyType::DB_KEY };
        if (sKeyFieldType == "UUID") {
            keyFieldType = FTSKeyType::UUID;
        }
        if (sKeyFieldType == "DBKEY") {
            keyFieldType = FTSKeyType::DB_KEY;
        }
        if (sKeyFieldType == "INT_ID") {
            keyFieldType = FTSKeyType::INT_ID;
        }
        return keyFieldType;
    }

    class FTSIndexSegment;

    using FTSIndexSegmentList = std::list<FTSIndexSegment>;
    using FTSIndexSegmentsMap = std::map<std::string, FTSIndexSegmentList>;


    class FTSPreparedIndexStmt final
    {
    public:
        FTSPreparedIndexStmt() = default;

        FTSPreparedIndexStmt(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view sql
        );

        FTSPreparedIndexStmt(const FTSPreparedIndexStmt&) = delete;
        FTSPreparedIndexStmt& operator=(const FTSPreparedIndexStmt&) = delete;

        FTSPreparedIndexStmt(FTSPreparedIndexStmt&&) noexcept = default;
        FTSPreparedIndexStmt& operator=(FTSPreparedIndexStmt&&) noexcept = default;

        Firebird::IStatement* getPreparedExtractRecordStmt()
        {
            return m_stmtExtractRecord;
        }

        Firebird::IMessageMetadata* getOutExtractRecordMetadata()
        {
            return m_outMetaExtractRecord;
        }

        Firebird::IMessageMetadata* getInExtractRecordMetadata()
        {
            return m_inMetaExtractRecord;
        }
    private:
        Firebird::AutoRelease<Firebird::IStatement> m_stmtExtractRecord;
        Firebird::AutoRelease<Firebird::IMessageMetadata> m_inMetaExtractRecord;
        Firebird::AutoRelease<Firebird::IMessageMetadata> m_outMetaExtractRecord;
    };

    /// <summary>
    /// Full-text index metadata.
    /// </summary>
    class FTSIndex final
    {
    public:
        std::string indexName;
        std::string relationName;
        std::string analyzer;
        std::string status; // N - new index, I - inactive, U - need rebuild, C - complete

        FTSIndexSegmentList segments;

        FTSKeyType keyFieldType{ FTSKeyType::NONE };
        std::wstring unicodeKeyFieldName;
        std::wstring unicodeIndexDir;
    public: 

        FTSIndex() = default;

        explicit FTSIndex(const FTSIndexRecord& record);

        // non-copyable
        FTSIndex(const FTSIndex& rhs) = delete;
        FTSIndex& operator=(const FTSIndex& rhs) = delete;
        // movable
        FTSIndex(FTSIndex&& rhs) noexcept = default;
        FTSIndex& operator=(FTSIndex&& rhs) noexcept = default;

        bool isActive() const {
            return (status == "C") || (status == "U");
        }

        bool emptySegments() const { 
            return segments.empty();
        }

        FTSIndexSegmentList::const_iterator findSegment(const std::string& fieldName) const;

        FTSIndexSegmentList::const_iterator findKey() const;

        bool checkAllFieldsExists() const;

        std::string buildSqlSelectFieldValues(
            Firebird::ThrowStatusWrapper* status,
            unsigned int sqlDialect,
            bool whereKey = false
        ) const;
    };


    using FTSIndexList = std::list<FTSIndex>;

    /// <summary>
    /// Metadata for a full-text index segment.
    /// </summary>
    class FTSIndexSegment final
    {

    public:
        FTSIndexSegment() = default;

        FTSIndexSegment(
            std::string_view indexName,
            std::string_view fieldName,
            bool key,
            double boost,
            bool boostNull,
            bool fieldExists
        );

        // non-copyable
        FTSIndexSegment(const FTSIndexSegment& rhs) = delete;
        FTSIndexSegment& operator=(const FTSIndexSegment& rhs) = delete;
        // movable
        FTSIndexSegment(FTSIndexSegment&& rhs) noexcept = default;
        FTSIndexSegment& operator=(FTSIndexSegment&& rhs) noexcept = default;

        bool compareFieldName(std::string_view fieldName) const {
            return (fieldName_ == fieldName) || (fieldName_ == "RDB$DB_KEY" && fieldName == "DB_KEY");
        }

        const std::string& indexName() const {
            return indexName_;
        }

        const std::string& fieldName() const {
            return fieldName_;
        }

        bool isKey() const {
            return key_;
        }

        double boost() const {
            return boost_;
        }

        bool isBoostNull() const {
            return boostNull_;
        }

        bool isFieldExists() const {
            return fieldExists_;
        }
    private:
        std::string indexName_;
        std::string fieldName_;
        bool key_ = false;
        double boost_ = 1.0;
        bool boostNull_ = true;
        bool fieldExists_ = false;
    };


    class RelationHelper;
    class AnalyzerRepository;


    /// <summary>
    /// Repository of full-text indexes. 
    /// 
    /// Allows you to retrieve and manage full-text index metadata.
    /// </summary>
    class FTSIndexRepository final
    {

    private:
        Firebird::IMaster* m_master = nullptr;
        AnalyzerRepository* m_analyzerRepository{ nullptr };
        RelationHelper* m_relationHelper{nullptr};
        // prepared statements
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_exists_index{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_index{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_index_fields{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_active_indexes_by_analyzer{ nullptr };

    public:

        FTSIndexRepository() = delete;


        explicit FTSIndexRepository(Firebird::IMaster* master);

        ~FTSIndexRepository();

        RelationHelper* getRelationHelper()
        {
            return m_relationHelper;
        }

        AnalyzerRepository* getAnalyzerRepository()
        {
            return m_analyzerRepository;
        }

        /// <summary>
        /// Create a new full-text index. 
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="relationName">Relation name</param>
        /// <param name="analyzerName">Analyzer name</param>
        /// <param name="description">Custom index description</param>
        void createIndex (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            std::string_view relationName,
            std::string_view analyzerName,
            ISC_QUAD* description);

        /// <summary>
        /// Remove a full-text index. 
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        void dropIndex (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName);

        /// <summary>
        /// Set the index status.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="indexStatus">Index Status</param>
        void setIndexStatus (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            std::string_view indexStatus);

        /// <summary>
        /// Checks if an index with the given name exists.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// 
        /// <returns>Returns true if the index exists, false otherwise</returns>
        bool hasIndex (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect, 
            std::string_view indexName);

        /// <summary>
        /// Returns index metadata by index name.
        /// 
        /// Throws an exception if the index does not exist. 
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="withSegments">Fill segments list</param>
        /// 
        FTSIndex getIndex (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect, 
            std::string_view indexName,
            bool withSegments = false);

        /// <summary>
        /// Returns a list of indexes. 
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="withSegments">Fill segments list</param>
        /// 
        FTSIndexList allIndexes (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            bool withSegments);


        /// <summary>
        /// Returns a list of index fields with the given name.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="segments">Segments list</param>
        /// 
        /// <returns>List of index fields (segments)</returns>
        void fillIndexFields(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            FTSIndexSegmentList& segments);



        /// <summary>
        /// Checks if an index key field exists for given relation.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// 
        /// <returns>Returns true if the index field exists, false otherwise</returns>
        bool hasKeyIndexField(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName
        );

        /// <summary>
        /// Adds a new field (segment) to the full-text index.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="key">Field is key</param>
        /// <param name="boost">Significance multiplier</param>
        /// <param name="boostNull">Boost null flag</param>
        void addIndexField (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            std::string_view fieldName,
            bool key,
            double boost = 1.0,
            bool boostNull = true);

        /// <summary>
        /// Removes a field (segment) from the full-text index.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="fieldName">Field name</param>
        void dropIndexField (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            std::string_view fieldName);

        /// <summary>
        /// Sets the significance multiplier for the index field.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="boost">Significance multiplier</param>
        /// <param name="boostNull">Boost null flag</param>
        void setIndexFieldBoost(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            std::string_view fieldName,
            double boost,
            bool boostNull = false);


        /// <summary>
        /// Checks for the existence of a field (segment) in a full-text index. 
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="indexName">Index name</param>
        /// <param name="fieldName">Field name</param>
        /// <returns>Returns true if the field (segment) exists in the index, false otherwise</returns>
        bool hasIndexField (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view indexName,
            std::string_view fieldName);


        bool hasIndexByAnalyzer(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view analyzerName
        );

        std::unordered_set<std::string> getActiveIndexByAnalyzer(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view analyzerName
        );
    };
    using FTSIndexRepositoryPtr = std::unique_ptr<FTSIndexRepository>;

}

#endif	// FTS_INDEX_H
