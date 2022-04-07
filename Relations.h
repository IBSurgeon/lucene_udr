#ifndef FTS_RELATIONS_H
#define FTS_RELATIONS_H

#include "LuceneUdr.h"
#include "FBUtils.h"
#include <string>
#include <sstream>
#include <list>

using namespace Firebird;
using namespace std;

namespace LuceneUDR
{
	class RelationHelper final
	{
	private:
		IMaster* m_master;
		// prepared statements
		AutoRelease<IStatement> stmt_exists_relation;
		AutoRelease<IStatement> stmt_exists_field;
	public:
		RelationHelper()
			: m_master(nullptr)
		{}

		RelationHelper(IMaster* master)
			: m_master(master)
		{}

		/// <summary>
		/// Checks if the given relation exists.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// 
		/// <returns>Returns true if the relation exists, false otherwise.</returns>
		bool relationExists(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &relationName);

		/// <summary>
		/// Checks if the specified column exists in the relation. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldName">Field name</param>
		/// 
		/// <returns>Returns true if the column exists, false otherwise.</returns>
		bool fieldExists(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &relationName,
			const string &fieldName);

		/// <summary>
		/// Builds an SQL query to retrieve field values. 
		/// </summary>
		/// 
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldNames">List of field names </param>
		/// <param name="whereDbKey">If true, then a filtering condition by RDB$DB_KEY is added, 
		/// otherwise an SQL query will be built without filtering, that is, returning all records.</param>
		/// 
		/// <returns>Returns the text of the SQL query.</returns>
		static inline string buildSqlSelectFieldValues(
			const unsigned int sqlDialect, 
			const string &relationName, 
			list<string> fieldNames, 
			const bool whereDbKey = false)
		{
			std::stringstream ss;
			ss << "SELECT\n";
			ss << "  RDB$DB_KEY";
			for (const auto fieldName : fieldNames) {
				ss << ",\n  " << escapeMetaName(sqlDialect, fieldName);
			}
			ss << "\nFROM " << escapeMetaName(sqlDialect, relationName);
			if (whereDbKey) {
				ss << "\nWHERE RDB$DB_KEY = ?";
			}
			return ss.str();
		}
	};
}

#endif	// FTS_RELATIONS_H
