#ifndef FB_AUTO_PTR_H
#define FB_AUTO_PTR_H

/*
 *  The contents of this file are subject to the Initial
 *  Developer's Public License Version 1.0 (the "License");
 *  you may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *  http://www.ibphoenix.com/main.nfs?a=ibphoenix&page=ibp_idpl.
 *
 *  Software distributed under the License is distributed AS IS,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied.
 *  See the License for the specific language governing rights
 *  and limitations under the License.
 *
 *  The Original Code was created by Adriano dos Santos Fernandes
 *  for the Firebird Open Source RDBMS project.
 *
 *  Copyright (c) 2015 Adriano dos Santos Fernandes <adrianosf@gmail.com>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
 */

namespace Firebird
{
    template <typename T>
    class AutoReleaseClear
    {
    public:
        static void clear(T* ptr)
        {
            if (ptr)
                ptr->release();
        }
    };

    template <typename T>
    class AutoDisposeClear
    {
    public:
        static void clear(T* ptr)
        {
            if (ptr)
                ptr->dispose();
        }
    };

    template <typename T>
    class AutoDeleteClear
    {
    public:
        static void clear(T* ptr)
        {
            delete ptr;
        }
    };

    template <typename T>
    class AutoArrayDeleteClear
    {
    public:
        static void clear(T* ptr)
        {
            delete[] ptr;
        }
    };

    template <typename T, typename Clear>
    class AutoImpl
    {
    public:
        AutoImpl<T, Clear>(T* aPtr = nullptr)
            : ptr(aPtr)
        {
        }

        ~AutoImpl()
        {
            Clear::clear(ptr);
        }

        // non-copyable
        AutoImpl<T, Clear>(AutoImpl<T, Clear>&) = delete;
        void operator=(AutoImpl<T, Clear>&) = delete;

        // movable
        AutoImpl<T, Clear>(AutoImpl<T, Clear>&& v) noexcept
            : ptr(v.ptr)
        {
            v.ptr = nullptr;
        }

        AutoImpl<T, Clear>& operator=(AutoImpl<T, Clear>&& r) noexcept
        {
            if (this != &r) {
                ptr = r.ptr;
                r.ptr = nullptr;
            }

            return *this;
        }

        AutoImpl<T, Clear>& operator =(T* aPtr)
        {
            Clear::clear(ptr);
            ptr = aPtr;
            return *this;
        }

        operator T* ()
        {
            return ptr;
        }

        operator const T* () const
        {
            return ptr;
        }

        bool operator !() const
        {
            return !ptr;
        }

        bool hasData() const
        {
            return ptr != nullptr;
        }

        T* operator ->()
        {
            return ptr;
        }

        T* release()
        {
            T* tmp = ptr;
            ptr = nullptr;
            return tmp;
        }

        void reset(T* aPtr = nullptr)
        {
            if (aPtr != ptr)
            {
                Clear::clear(ptr);
                ptr = aPtr;
            }
        }

    private:
        T* ptr;
    };

    template <typename T> class AutoDispose : public AutoImpl<T, AutoDisposeClear<T> >
    {
    public:
        AutoDispose(T* ptr = nullptr)
            : AutoImpl<T, AutoDisposeClear<T> >(ptr)
        {
        }
    };

    template <typename T> class AutoRelease : public AutoImpl<T, AutoReleaseClear<T> >
    {
    public:
        AutoRelease(T* ptr = nullptr)
            : AutoImpl<T, AutoReleaseClear<T> >(ptr)
        {
        }
    };

    template <typename T> class AutoDelete : public AutoImpl<T, AutoDeleteClear<T> >
    {
    public:
        AutoDelete(T* ptr = nullptr)
            : AutoImpl<T, AutoDeleteClear<T> >(ptr)
        {
        }
    };

    template <typename T> class AutoArrayDelete : public AutoImpl<T, AutoArrayDeleteClear<T> >
    {
    public:
        AutoArrayDelete(T* ptr = nullptr)
            : AutoImpl<T, AutoArrayDeleteClear<T> >(ptr)
        {
        }
    };
}

#endif	// FB_AUTO_PTR_H
