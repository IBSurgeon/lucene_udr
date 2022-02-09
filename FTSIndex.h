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

		FTSIndex index;

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
		AutoRelease<IStatement> stmt_rel_segments;
	public:
		FTSIndexRepository()
			: FTSIndexRepository(nullptr)
		{}

		FTSIndexRepository(IMaster* master)
			: m_master(master),
			stmt_exists_index(nullptr),
			stmt_get_index(nullptr),
			stmt_index_segments(nullptr),
			stmt_rel_segments(nullptr)
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
		// Возвращает сегменты индексов по имени таблицы
		//
		list<FTSIndexSegment> getIndexSegmentsByRelation(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string relationName);

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
				}
				else {
					list<FTSIndexSegment> relSegments;
					relSegments.push_back(segment);
					segmentsByRelation[segment.relationName] = relSegments;
				}
			}
			return segmentsByRelation;
		}

		//
        // Группирует сегменты индексов по именам индекса 
        //
		static inline map<string, list<FTSIndexSegment>> groupSegmentsByIndex(list<FTSIndexSegment> segments)
		{
			map<string, list<FTSIndexSegment>> segmentsByIndex;
			for (const auto& segment : segments) {
				auto r = segmentsByIndex.find(segment.indexName);
				if (r != segmentsByIndex.end()) {
					auto idxSegments = r->second;
					idxSegments.push_back(segment);
				}
				else {
					list<FTSIndexSegment> idxSegments;
					idxSegments.push_back(segment);
					segmentsByIndex[segment.indexName] = idxSegments;
				}
			}
			return segmentsByIndex;
		}
	};
}
#endif	// FTS_INDEX_H
