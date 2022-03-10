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
		// подготовленные запросы
		AutoRelease<IStatement> stmt_exists_relation;
		AutoRelease<IStatement> stmt_exists_field;
	public:
		RelationHelper()
			: m_master(nullptr)
		{}

		RelationHelper(IMaster* master)
			: m_master(master)
		{}

		bool relationExists(
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			unsigned int sqlDialect, 
			string relationName);

		bool fieldExists(
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			unsigned int sqlDialect, 
			string relationName, 
			string fieldName);

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
