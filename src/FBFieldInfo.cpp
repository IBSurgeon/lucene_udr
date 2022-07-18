#include "FBFieldInfo.h"
#include "FBUtils.h"

using namespace std;
using namespace Firebird;

namespace LuceneUDR
{
	string FbFieldInfo::getStringValue(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned char* buffer)
	{
		switch (dataType) {
		case SQL_TEXT:
		case SQL_VARYING:
		{
			string s(getCharValue(buffer), getOctetsLength(buffer));
			return s;
		}
		case SQL_BLOB:
		{
			ISC_QUAD blobId = getQuadValue(buffer);
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &blobId, 0, nullptr));
			string s = BlobUtils::getString(status, blob);
			blob->close(status);
			return s;
		}
		case SQL_SHORT:
		{
			if (!scale) {
				return std::to_string(getShortValue(buffer));
			}
		}
		case SQL_LONG:
		{
			if (!scale) {
				return std::to_string(getLongValue(buffer));
			}
		}
		case SQL_INT64:
		{
			if (!scale) {
				return std::to_string(getInt64Value(buffer));
			}
		}
		default:
			// Other types are not considered yet.
			return "";
		}
	}

	FbFieldsInfo::FbFieldsInfo(ThrowStatusWrapper* status, IMessageMetadata* const meta)
		: FbFieldInfoVector()
		, m_fieldByNameMap()
	{
		const auto fieldCount = meta->getCount(status);
		for (unsigned i = 0; i < fieldCount; i++) {
			auto field = make_unique<FbFieldInfo>();
			field->fieldIndex = i;
			field->nullable = meta->isNullable(status, i);
			field->fieldName.assign(meta->getField(status, i));
			field->relationName.assign(meta->getRelation(status, i));
			field->owner.assign(meta->getOwner(status, i));
			field->alias.assign(meta->getAlias(status, i));
			field->dataType = meta->getType(status, i);
			field->subType = meta->getSubType(status, i);
			field->length = meta->getLength(status, i);
			field->scale = meta->getScale(status, i);
			field->charSet = meta->getCharSet(status, i);
			field->offset = meta->getOffset(status, i);
			field->nullOffset = meta->getNullOffset(status, i);
			this->push_back(std::move(field));
		}
		for (unsigned i = 0; i < fieldCount; i++) {
			const auto& field = this->at(i);
			m_fieldByNameMap[field->fieldName] = i;
		}
	}

	int FbFieldsInfo::findFieldByName(const string& fieldName)
	{
		auto it = m_fieldByNameMap.find(fieldName);
		if (it != m_fieldByNameMap.end()) {
			return it->second;
		}
		return -1;
	}
}