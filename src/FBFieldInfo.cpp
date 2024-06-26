/**
 *  Utilities for getting information about query fields and their values.
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

#include "FBFieldInfo.h"
#include "FBUtils.h"

using namespace Firebird;
using namespace LuceneUDR;

namespace FTSMetadata
{
    FbFieldInfo::FbFieldInfo(ThrowStatusWrapper* status, IMessageMetadata* const meta, unsigned index)
    {
        fieldIndex = index;
        nullable = meta->isNullable(status, index);
        fieldName.assign(meta->getField(status, index));
        relationName.assign(meta->getRelation(status, index));
        owner.assign(meta->getOwner(status, index));
        alias.assign(meta->getAlias(status, index));
        dataType = meta->getType(status, index);
        subType = meta->getSubType(status, index);
        length = meta->getLength(status, index);
        scale = meta->getScale(status, index);
        charSet = meta->getCharSet(status, index);
        offset = meta->getOffset(status, index);
        nullOffset = meta->getNullOffset(status, index);
    }

    std::string FbFieldInfo::getStringValue(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned char* buffer) const
    try
    {
        if (isNull(buffer)) {
            return {};
        }
        switch (dataType) {
        case SQL_TEXT:
        case SQL_VARYING:
        {
            if (charSet != CS_BINARY) {
                return { getCharValue(buffer), static_cast<size_t>(getOctetsLength(buffer)) };
            }
            else {
                return binary_to_hex(getBinaryValue(buffer), static_cast<size_t>(getOctetsLength(buffer)));
            }
            
        }
        case SQL_BLOB:
        {
            ISC_QUAD* blobIdPtr = getQuadPtr(buffer);
            if (subType == 1) {
                return readStringFromBlob(status, att, tra, blobIdPtr);
            }
            else {
                auto v = readBinaryFromBlob(status, att, tra, blobIdPtr);
                return binary_to_hex(v.data(), v.size());
            }
        }
        default:
            // Other types are not considered yet.
            return {};
        }
    }
    catch (const std::exception& e) {
        IscRandomStatus st(e);
        throw Firebird::FbException(status, st);
    }

    FbFieldsInfo::FbFieldsInfo(ThrowStatusWrapper* status, IMessageMetadata* const meta)
        : FbFieldInfoVector()
        , m_fieldByNameMap()
    {
        const auto fieldCount = meta->getCount(status);
        reserve(fieldCount);
        for (unsigned i = 0; i < fieldCount; i++) {
            auto&& field = emplace_back(status, meta, i);
            m_fieldByNameMap[field.fieldName] = i;
        }
    }

    int FbFieldsInfo::findFieldByName(const std::string& fieldName) const
    {
        auto it = m_fieldByNameMap.find(fieldName);
        if (it != m_fieldByNameMap.end()) {
            return it->second;
        }
        return -1;
    }
}
