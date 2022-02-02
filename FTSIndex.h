#ifndef FTS_INDEX_H
#define FTS_INDEX_H

#include "LuceneUdr.h"
#include <string>
#include <list>
#include <map>

using namespace Firebird;
using namespace std;

namespace LuceneFTS
{

	struct FTSIndex 
	{
		string indexName;
		string analyzer;
		string description;
	};

	struct FTSIndexSegment 
	{
		string indexName;
		string relationName;
		string fieldName;

		string getFullFieldName() {
			return relationName + "." + fieldName;
		}
	};

	class FTSIndexRepository final
	{
	private:
		IMaster* m_master;
		// подготовленные запросы
		AutoRelease<IStatement> stmt_exists_index;
		AutoRelease<IStatement> stmt_get_index;
		AutoRelease<IStatement> stmt_index_segments;
	public:
		FTSIndexRepository()
			: m_master(nullptr)
		{}

		FTSIndexRepository(IMaster* master)
			: m_master(master)
		{
		}

		//
		// Создание нового полнотекстового индекса
		//
		void createIndex(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string indexName,
			string analyzer,
			string description);

		//
		// Удаление полнотекстового индекса
		//
		void dropIndex(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string indexName);

		//
		// Возвращает существует ли индекс с заданным именем.
		//
		bool hasIndex(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string indexName);

		//
		// Получает информацию об индексе с заданным именем, если он существует.
		// Возвращает true, если индекс существует, и false - в противном случае.
		//
		bool getIndex(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string indexName, FTSIndex& ftsIndex);

		//
		// Возвращает сегменты индекса с заданным именем
		//
		list<FTSIndexSegment> getIndexSegments(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string indexName);

		//
		// Добавление нового поля (сегмента) полнотекстового индекса
		//
		void addIndexField(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string indexName,
			string relationName,
			string fieldName);

		//
		// Удаление поля (сегмента) из полнотекстового индекса
		//
		void dropIndexField(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string indexName,
			string relationName,
			string fieldName);

		//
		// Проверка существования поля (сегмента) в полнотекстовом индексе
		//
		bool hasIndexSegment(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string indexName,
			string relationName,
			string fieldName);

		//
		// Группирует сегменты индекса по именам таблиц 
		//
		static inline map<string, list<FTSIndexSegment>> groupIndexSegmentsByRelation(list<FTSIndexSegment> segments)
		{
			map<string, list<FTSIndexSegment>> segmentsByRelation;
			for (const auto& segment : segments) {			
				auto r = segmentsByRelation.find(segment.relationName);
				if (r != segmentsByRelation.end()) {
					auto relSegments = r->second;
					relSegments.push_back(segment);
				} else {
					list<FTSIndexSegment> relSegments;
					relSegments.push_back(segment);
					segmentsByRelation[segment.relationName] = relSegments;
				}
			}
			return segmentsByRelation;
		}
	};
}
#endif	// FTS_INDEX_H
