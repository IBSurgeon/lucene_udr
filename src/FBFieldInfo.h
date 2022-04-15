#ifndef FB_FIELD_INFO_H
#define FB_FIELD_INFO_H

#include <string>
#include <vector>
#include "FBUtils.h"
#include "FBAutoPtr.h"
#include "firebird/UdrCppEngine.h"

using namespace std;
using namespace Firebird;

namespace LuceneUDR
{

	template <typename T>
	inline T as(unsigned char* ptr)
	{
		return *((T*)ptr);
	}

	template <typename T>
	inline string ToString(T tX)
	{
		std::ostringstream oStream;
		oStream << tX;
		return oStream.str();
	}

	struct FbFieldInfo {
		unsigned fieldIndex;

		string fieldName;
		string relationName;
		string owner;
		string alias;
		bool nullable;
		unsigned dataType;
		int subType;
		unsigned length;
		int scale;
		unsigned charSet;
		unsigned offset;
		unsigned nullOffset;

		FbFieldInfo()
			: fieldIndex(0)
			, fieldName("")
			, relationName("")
			, owner("")
			, nullable(false)
			, dataType(0)
			, subType(0)
			, length(0)
			, scale(0)
			, charSet(0)
			, offset(0)
			, nullOffset(0)
		{
		}

		inline bool isNull(unsigned char* buffer) {
			return as<short>(buffer + nullOffset);
		}

		inline FB_BOOLEAN getBooleanValue(unsigned char* buffer) {
			return as<FB_BOOLEAN>(buffer + offset);
		}

		inline ISC_SHORT getShortValue(unsigned char* buffer) {
			return as<ISC_SHORT>(buffer + offset);
		}

		inline ISC_LONG getLongValue(unsigned char* buffer) {
			return as<ISC_LONG>(buffer + offset);
		}

		inline ISC_INT64 getInt64Value(unsigned char* buffer) {
			return as<ISC_INT64>(buffer + offset);
		}

		inline FB_I128 getInt128Value(unsigned char* buffer) {
			return as<FB_I128>(buffer + offset);
		}

		inline float getFloatValue(unsigned char* buffer) {
			return as<float>(buffer + offset);
		}

		inline double getDoubleValue(unsigned char* buffer) {
			return as<double>(buffer + offset);
		}

		inline FB_DEC16 getDecFloat16Value(unsigned char* buffer) {
			return as<FB_DEC16>(buffer + offset);
		}

		inline FB_DEC34 getDecFloat34Value(unsigned char* buffer) {
			return as<FB_DEC34>(buffer + offset);
		}

		inline ISC_DATE getDateValue(unsigned char* buffer) {
			return as<ISC_DATE>(buffer + offset);
		}

		inline ISC_TIME getTimeValue(unsigned char* buffer) {
			return as<ISC_TIME>(buffer + offset);
		}

		inline ISC_TIME_TZ getTimeTzValue(unsigned char* buffer) {
			return as<ISC_TIME_TZ>(buffer + offset);
		}

		inline ISC_TIME_TZ_EX getTimeTzExValue(unsigned char* buffer) {
			return as<ISC_TIME_TZ_EX>(buffer + offset);
		}

		inline ISC_TIMESTAMP getTimestampValue(unsigned char* buffer) {
			return as<ISC_TIMESTAMP>(buffer + offset);
		}

		inline ISC_TIMESTAMP_TZ getTimestampTzValue(unsigned char* buffer) {
			return as<ISC_TIMESTAMP_TZ>(buffer + offset);
		}

		inline ISC_TIMESTAMP_TZ_EX getTimestampTzExValue(unsigned char* buffer) {
			return as<ISC_TIMESTAMP_TZ_EX>(buffer + offset);
		}

		inline ISC_QUAD getQuadValue(unsigned char* buffer) {
			return as<ISC_QUAD>(buffer + offset);
		}

		inline short getOctetsLength(unsigned char* buffer) {
			switch (dataType)
			{
			case SQL_TEXT:
				return length;
			case SQL_VARYING:
				return as<short>(buffer + offset);
			default:
				return 0;
			}
		}

		inline char* getCharValue(unsigned char* buffer) {
			switch (dataType)
			{
			case SQL_TEXT:
				return (char*)(buffer + offset);
			case SQL_VARYING:
				return (char*)(buffer + offset + sizeof(short));
			default:
				return nullptr;
			}
		}

		template <class StatusType>
		string getStringValue(StatusType* status, IAttachment* att, ITransaction* tra, unsigned char* buffer);
	};

	template <class StatusType>
	string FbFieldInfo::getStringValue(StatusType* status, IAttachment* att, ITransaction* tra, unsigned char* buffer)
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
			string s = blob_get_string(status, blob);
			blob->close(status);
			return s;
		}
		default:
			// Other types are not considered yet.
			return "";
		}
	}

	using FbFieldInfoVector = vector<FbFieldInfo>;

	class FbFieldsInfo : public FbFieldInfoVector
	{
	public:
		template <class StatusType>
		FbFieldsInfo(StatusType* status, IMessageMetadata* meta)
			: FbFieldInfoVector(meta->getCount(status))
		{
			for (unsigned i = 0; i < size(); i++) {
				FbFieldInfo field;
				field.fieldIndex = i;
				field.nullable = meta->isNullable(status, i);
				field.fieldName.assign(meta->getField(status, i));
				field.relationName.assign(meta->getRelation(status, i));
				field.owner.assign(meta->getOwner(status, i));
				field.alias.assign(meta->getAlias(status, i));
				field.dataType = meta->getType(status, i);
				field.subType = meta->getSubType(status, i);
				field.length = meta->getLength(status, i);
				field.scale = meta->getScale(status, i);
				field.charSet = meta->getCharSet(status, i);
				field.offset = meta->getOffset(status, i);
				field.nullOffset = meta->getNullOffset(status, i);
				at(i) = field;
			}
		}

		int findFieldByName(const string fieldName)
		{
			for (const auto& fieldInfo : *this) {
				if (fieldInfo.fieldName == fieldName) {
					return fieldInfo.fieldIndex;
				}
			}
			return -1;
		}
	};

}


#endif	// FB_FIELD_INFO_H