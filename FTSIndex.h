#ifndef FTS_INDEX_H
#define FTS_INDEX_H

#include "LuceneUdr.h"
#include "Relations.h"
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
		string status; // N - new index, I - inactive, U - need rebuild, C - complete

		bool isActive() {
			return (status == "C") || (status == "U");
		}
	};

	struct FTSIndexSegment
	{
		string indexName;
		string relationName;
		string fieldName;
		bool storeData;
		double boost = 1.0;

		FTSIndex index;

		string getFullFieldName() {
			return relationName + "." + fieldName;
		}
	};

	string getFtsDirectory(IExternalContext* context);

	class FTSRelation {

	private:
		string _relationName;                   // имя таблицы

		map<string, FTSIndex> _indexes;         // индексы по имени
		map<string, list<FTSIndexSegment>> _segments; // сегменты индекса для данной таблицы по индексам
		map<string, string> _sqls;              // SQL запросы по именам индексов для данной таблицы

		void addRelationSegments(const string indexName)
		{
			auto r = _segments.find(indexName);
			if (r == _segments.end()) {
				_segments[indexName] = list<FTSIndexSegment>();
			}
		}
	public:
		FTSRelation(string relationName) 
			: _relationName(relationName),
			_indexes(),
			_segments(),
			_sqls()
		{
		}

		void addIndex(FTSIndex index)
		{
			auto r = _indexes.find(index.indexName);
			if (r == _indexes.end()) {
				_indexes[index.indexName] = index;
			}
		}

		void updateIndex(FTSIndex index)
		{
			_indexes[index.indexName] = index;
		}

		map<string, FTSIndex> getIndexes()
		{
			return _indexes;
		}

		void setSql(const string indexName, const string sql)
		{
			_sqls[indexName] = sql;
		}

		const string getSql(const string indexName) {
			return _sqls[indexName];
		}

		void addSegment(FTSIndexSegment segment)
		{
			addRelationSegments(segment.indexName);
			_segments[segment.indexName].push_back(segment);
		}

		list<FTSIndexSegment> getSegmentsByIndexName(const string indexName)
		{
			return _segments[indexName];
		}
	};

	class FTSIndexRepository final
	{
	private:
		IMaster* m_master;
		RelationHelper relationHelper;
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
			relationHelper(master),
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
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			unsigned int sqlDialect,
			string indexName,
			string analyzer,
			string description);

		//
		// Удаление полнотекстового индекса
		//
		void dropIndex(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			unsigned int sqlDialect,
			string indexName);

		//
		// Устанавливает статус индекса
		//
		void setIndexStatus(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			unsigned int sqlDialect,
			string indexName,
			string indexStatus);

		//
		// Возвращает существует ли индекс с заданным именем.
		//
		bool hasIndex(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, string indexName);

		//
		// Получает информацию об индексе с заданным именем, если он существует.
		// Возвращает true, если индекс существует, и false - в противном случае.
		//
		FTSIndex getIndex(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, string indexName);

		//
		// Возвращет список всех индексов
		//
		list<FTSIndex> getAllIndexes(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect);

		//
		// Возвращает сегменты индекса с заданным именем
		//
		list<FTSIndexSegment> getIndexSegments(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, string indexName);

		//
        // Возвращает все сегменты всех индексов. Упорядочено по имени индекса
        //
		list<FTSIndexSegment> getAllIndexSegments(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect);

		//
		// Возвращает сегменты индексов по имени таблицы
		//
		list<FTSIndexSegment> getIndexSegmentsByRelation(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, string relationName);

		//
		// Добавление нового поля (сегмента) полнотекстового индекса
		//
		void addIndexField(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			unsigned int sqlDialect,
			string indexName,
			string relationName,
			string fieldName,
			double boost);

		//
		// Удаление поля (сегмента) из полнотекстового индекса
		//
		void dropIndexField(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			unsigned int sqlDialect,
			string indexName,
			string relationName,
			string fieldName);

		//
		// Проверка существования поля (сегмента) в полнотекстовом индексе
		//
		bool hasIndexSegment(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			unsigned int sqlDialect,
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
					segmentsByRelation[segment.relationName] = relSegments;
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
					segmentsByIndex[segment.indexName] = idxSegments;
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
