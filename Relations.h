#ifndef FTS_RELATIONS_H
#define FTS_RELATIONS_H

#include "LuceneUdr.h"
#include <string>
#include <list>

using namespace Firebird;
using namespace std;

namespace LuceneFTS
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
			unsigned int sqlDialect,
			string relationName);

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
			unsigned int sqlDialect,
			string relationName,
			string fieldName);

		/// <summary>
		/// Escapes the name of the metadata object depending on the SQL dialect. 
		/// </summary>
		/// 
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="name">Metadata object name</param>
		/// 
		/// <returns>Returns the escaped name of the metadata object.</returns>
		static inline string escapeMetaName(unsigned int sqlDialect, const string name)
		{
			switch (sqlDialect) {
			case 1:
				return name;
			case 3:
			default:
				return "\"" + name + "\"";
			}
		}

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
		static inline string buildSqlSelectFieldValues(unsigned int sqlDialect, string relationName, list<string> fieldNames, bool whereDbKey = false)
		{
			relationName = RelationHelper::escapeMetaName(sqlDialect, relationName);
			std::stringstream ss;
			ss << "SELECT\n";
			ss << "  RDB$DB_KEY";
			for (auto fieldName : fieldNames) {
				ss << ",\n  " << RelationHelper::escapeMetaName(sqlDialect, fieldName);
			}
			ss << "\nFROM " << relationName;
			if (whereDbKey) {
				ss << "\nWHERE RDB$DB_KEY = ?";
			}
			return ss.str();
		}
	};
}

#endif	// FTS_RELATIONS_H
