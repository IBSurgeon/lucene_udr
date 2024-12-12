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
	template <typename What>
	class SimpleDelete
	{
	public:
		static void clear(What* ptr)
		{
			delete ptr;
		}
	};

	template <typename What>
	class ArrayDelete
	{
	public:
		static void clear(What* ptr)
		{
			delete[] ptr;
		}
	};


	template <typename T>
	class SimpleRelease
	{
	public:
		static void clear(T* ptr)
		{
			if (ptr)
			{
				ptr->release();
			}
		}
	};


	template <typename T>
	class SimpleDispose
	{
	public:
		static void clear(T* ptr)
		{
			if (ptr)
			{
				ptr->dispose();
			}
		}
	};


	template <typename Where, template <typename W> class Clear = SimpleDelete >
	class AutoPtr
	{
	private:
		Where* ptr;
	public:
		AutoPtr(Where* v = nullptr) noexcept
			: ptr(v)
		{}

		AutoPtr(AutoPtr&) = delete;

		AutoPtr(AutoPtr&& v) noexcept
			: ptr(v.ptr)
		{
			v.ptr = nullptr;
		}

		~AutoPtr()
		{
			Clear<Where>::clear(ptr);
		}

		void operator=(AutoPtr&) = delete;

		AutoPtr& operator=(AutoPtr&& r) noexcept
		{
			if (this != &r)
			{
				ptr = r.ptr;
				r.ptr = nullptr;
			}

			return *this;
		}

		AutoPtr& operator= (Where* v)
		{
			Clear<Where>::clear(ptr);
			ptr = v;
			return *this;
		}

		operator Where* ()
		{
			return ptr;
		}

		Where* get()
		{
			return ptr;
		}

		operator const Where* () const
		{
			return ptr;
		}

		const Where* get() const
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

		Where* operator->()
		{
			return ptr;
		}

		const Where* operator->() const
		{
			return ptr;
		}

		Where* release()
		{
			Where* tmp = ptr;
			ptr = nullptr;
			return tmp;
		}

		void reset(Where* v = nullptr)
		{
			if (v != ptr)
			{
				Clear<Where>::clear(ptr);
				ptr = v;
			}
		}
	};


	template <typename T>
	using AutoDispose = AutoPtr<T, SimpleDispose>;

	template <typename T>
	using AutoRelease = AutoPtr<T, SimpleRelease>;

}

#endif // FB_AUTO_PTR_H
