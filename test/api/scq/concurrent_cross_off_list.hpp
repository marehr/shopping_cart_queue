
#pragma once

#include <mutex>
#include <set>

template <typename item_t>
struct concurrent_cross_off_list
{
    concurrent_cross_off_list(std::initializer_list<item_t> list)
        : _set{list}
    {}

    // returns true if item was crossed out
    bool cross_off(item_t item)
    {
        std::unique_lock lk{_set_mutex};

        auto it = _set.find(item);
        bool crossed_off = it != _set.end();

        if (crossed_off)
            _set.erase(it);

        return crossed_off;
    }

    bool empty()
    {
        return _set.empty();
    }

private:
    std::set<item_t> _set;

    std::mutex _set_mutex{};
};
