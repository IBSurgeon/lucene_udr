#ifndef FB_LAZY_FACTORY_H
#define FB_LAZY_FACTORY_H

/**
 *  Lazy factory for unique_ptr and shared_ptr.
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

#include <memory>

template<class Factory>
struct lazy_convert_construct
{
	using result_type = std::invoke_result_t<const Factory&>;

	constexpr lazy_convert_construct(Factory&& factory)
		: factory_(std::move(factory))
	{
	}

	constexpr operator result_type() const noexcept(std::is_nothrow_invocable_v<const Factory&>)
	{
		return factory_();
	}

	Factory factory_;
};

#endif
