#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

#pragma once

namespace scq
{

struct slot_count
{
    std::size_t slot_count;
};

struct cart_count
{
    std::size_t cart_count;
};

struct cart_capacity
{
    std::size_t cart_capacity;
};

struct slot_id
{
    std::size_t slot_id;
};

template <typename value_t>
class slotted_cart_queue;

template <typename value_t>
class cart
{
public:
    cart() = default;
    cart(cart const &) = delete;
    cart(cart &&) = default;
    cart & operator=(cart const &) = delete;
    cart & operator=(cart &&) = default;

    using value_type = value_t;

    bool valid() const
    {
        return true;
    }

    std::pair<scq::slot_id, std::span<value_type>> get()
    {
        return {_id, std::span<value_type>{_cart_memory, 1}};
    }

private:
    template <typename>
    friend class slotted_cart_queue;

    scq::slot_id _id{};
    value_type _cart_memory[1]{};
};

template <typename value_t>
class slotted_cart_queue
{
public:
    using value_type = value_t;
    using cart_type = cart<value_type>;

    slotted_cart_queue() = default;
    slotted_cart_queue(slotted_cart_queue const &) = delete;
    slotted_cart_queue(slotted_cart_queue &&) = delete; // TODO:
    slotted_cart_queue & operator=(slotted_cart_queue const &) = delete;
    slotted_cart_queue & operator=(slotted_cart_queue &&) = delete; // TODO:

    slotted_cart_queue(slot_count slots, cart_count carts, cart_capacity cart_capacity)
        : _slot_count{slots.slot_count},
          _cart_count{carts.cart_count},
          _cart_capacity{cart_capacity.cart_capacity}
    {
        if (_cart_count < _slot_count)
            throw std::logic_error{"The number of carts must be >= the number of slots."};

        if (_cart_capacity == 0u)
            throw std::logic_error{"The cart capacity must be >= 1."};
    }

    void enqueue(slot_id slot, value_t value)
    {
        bool queue_was_empty{};
        bool queue_was_closed{};

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            queue_was_closed = _queue_closed;
            queue_was_empty = _cart_memory.empty();

            if (!queue_was_closed)
            {
                _cart_memory.emplace_back(slot, std::move(value));
            }
        }

        if (queue_was_empty)
            _queue_empty_cv.notify_all();

        if (queue_was_closed)
            throw std::overflow_error{"Queue is already closed."};
    }

    cart_type dequeue()
    {
        cart_type cart{};
        _cart_type _tmp_cart;

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            _queue_empty_cv.wait(cart_management_lock, [this]
            {
                return _cart_memory.empty() == false;
            });

            _tmp_cart = _cart_memory.back();
            _cart_memory.pop_back();
        }

        // prepare return data after critical section
        cart._id = _tmp_cart.first;
        cart._cart_memory[0] = _tmp_cart.second;

        return cart;
    }

    void close()
    {
        std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

        _queue_closed = true;
    }

private:
    std::size_t _slot_count{};
    std::size_t _cart_count{};
    std::size_t _cart_capacity{};

    using _cart_type = std::pair<slot_id, value_t>;

    bool _queue_closed{false};
    std::vector<_cart_type> _cart_memory{};

    std::mutex _cart_management_mutex;
    std::condition_variable _queue_empty_cv;
};

} // namespace scq
