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

		bool relationExists(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string relationName);
		bool fieldExists(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string relationName, string fieldName);

		static inline string buildSqlSelectFieldValues(string relationName, list<string> fieldNames)
		{
			std::stringstream ss;
			ss << "SELECT\n";
			ss << "  RDB$DB_KEY";
			for (auto fieldName : fieldNames) {
				ss << ",\n  " << fieldName;
			}
			ss << "\nFROM " << relationName;
			return ss.str();
		}
	};
}

#endif	// FTS_RELATIONS_H
