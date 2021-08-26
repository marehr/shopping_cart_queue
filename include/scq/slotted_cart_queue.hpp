#include <condition_variable>
#include <cstddef> // std::size_t
#include <future> // future_error
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
        return _valid == true;
    }

    std::pair<scq::slot_id, std::span<value_type>> get()
    {
        if (!valid()) // slotted_cart_queue is already closed and no further elements.
            throw std::future_error{std::future_errc::no_state};

        return {_id, std::span<value_type>{_cart_span.data(), _cart_span.size()}};
    }

private:
    template <typename>
    friend class slotted_cart_queue;

    scq::slot_id _id{};
    std::vector<value_type> _cart_span{};
    bool _valid{true};
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
          _cart_capacity{cart_capacity.cart_capacity},
          _to_fill_carts(slots.slot_count) // default init slot_count many vectors
    {
        if (_cart_count < _slot_count)
            throw std::logic_error{"The number of carts must be >= the number of slots."};

        if (_cart_capacity == 0u)
            throw std::logic_error{"The cart capacity must be >= 1."};
    }

    void enqueue(slot_id slot, value_type value)
    {
        bool queue_was_empty{};
        bool queue_was_closed{};

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            _queue_full_or_closed_cv.wait(cart_management_lock, [this]
            {
                return !full_slot_cart_queue_is_full() || _queue_closed == true;
            });

            queue_was_closed = _queue_closed;
            queue_was_empty = full_slot_cart_queue_is_empty();

            if (!queue_was_closed)
            {
                add_value_to_slot_cart(slot, std::move(value));

                if (slot_cart_is_full(slot))
                    move_slot_cart_into_full_cart_queue(slot);
            }
        }

        if (queue_was_empty)
            _queue_empty_or_closed_cv.notify_all();

        if (queue_was_closed)
            throw std::overflow_error{"slotted_cart_queue is already closed."};
    }

    cart_type dequeue()
    {
        _cart_type _tmp_cart{};

        bool queue_was_full{};
        bool queue_was_empty{};

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            _queue_empty_or_closed_cv.wait(cart_management_lock, [this]
            {
                // wait until first cart is full
                return !full_slot_cart_queue_is_empty() || _queue_closed == true;
            });

            queue_was_empty = full_slot_cart_queue_is_empty();
            queue_was_full = full_slot_cart_queue_is_full();

            if (!queue_was_empty)
            {
                _tmp_cart = dequeue_slot_cart_from_full_cart_queue();
            }
        }

        //
        // prepare return data after critical section
        //

        if (queue_was_full)
            _queue_full_or_closed_cv.notify_all();

        // NOTE: this also handles queue_was_empty; if queue_was_empty we return a no_state cart
        // this has a asymmetric behaviour from enqueue as we assume multiple "polling" (dequeue) threads. The queue
        // should be closed after all the data was pushed.
        return create_cart_future(_tmp_cart, queue_was_empty);
    }

    void close()
    {
        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            _queue_closed = true;
            move_non_empty_slot_carts_into_full_cart_queue();
        }

        _queue_empty_or_closed_cv.notify_all();
        _queue_full_or_closed_cv.notify_all();
    }

private:
    std::size_t _slot_count{};
    std::size_t _cart_count{};
    std::size_t _cart_capacity{};

    using _cart_type = std::pair<slot_id, std::vector<value_type>>;

    void move_non_empty_slot_carts_into_full_cart_queue()
    {
        // TODO: if pending slots are more than queue capacity? is that a problem?

        // put all non-full carts into full queue (no element can't be added any more and all pending elements =
        // active to fill elements must be processed)
        for (size_t slot_id = 0u; slot_id < _to_fill_carts.size(); ++slot_id)
        {
            scq::slot_id slot{slot_id};
            if (!slot_cart_is_empty(slot))
                move_slot_cart_into_full_cart_queue(slot);
        }
    }

    cart_type create_cart_future(_cart_type tmp_cart, bool queue_was_empty)
    {
        cart_type cart{};
        cart._id = tmp_cart.first;
        cart._cart_span = std::move(tmp_cart.second); // TODO: memory should be owned by the queue not the cart
        cart._valid = !queue_was_empty;
        return cart;
    }

    void add_value_to_slot_cart(scq::slot_id slot, value_type value)
    {
        std::vector<value_type> & slot_cart = _to_fill_carts[slot.slot_id];

        assert(slot_cart.size() < _cart_capacity);

        slot_cart.emplace_back(std::move(value));
    }

    _cart_type dequeue_slot_cart_from_full_cart_queue()
    {
        _cart_type tmp = std::move(_cart_memory.back());
        _cart_memory.pop_back();
        return tmp;
    }

    bool full_slot_cart_queue_is_empty()
    {
        return _cart_memory.empty();
    }

    bool full_slot_cart_queue_is_full()
    {
        return _cart_memory.size() >= _slot_count;
    }

    bool slot_cart_is_empty(scq::slot_id slot)
    {
        auto & slot_cart = _to_fill_carts[slot.slot_id];
        return slot_cart.empty();
    }

    bool slot_cart_is_full(scq::slot_id slot)
    {
        auto & slot_cart = _to_fill_carts[slot.slot_id];
        return slot_cart.size() >= _cart_capacity;
    }

    void move_slot_cart_into_full_cart_queue(scq::slot_id slot)
    {
        auto & slot_cart = _to_fill_carts[slot.slot_id];

        assert(slot_cart.size() > 0); // at least one element
        assert(slot_cart.size() <= _cart_capacity); // at most cart capacity many elements

        _cart_memory.emplace_back(slot, std::move(slot_cart));
        slot_cart = {}; // reset slotted cart
    }

    bool _queue_closed{false};
    std::vector<_cart_type> _cart_memory{};

    std::vector<std::vector<value_type>> _to_fill_carts{}; // position is slot_id

    std::mutex _cart_management_mutex;
    std::condition_variable _queue_empty_or_closed_cv;
    std::condition_variable _queue_full_or_closed_cv;
};

} // namespace scq
