#pragma once

#include <cassert>
#include <condition_variable>
#include <cstddef> // std::size_t
#include <future> // future_error
#include <mutex>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

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
    ~cart()
    {
        if (valid())
            _cart_queue->notify_processed_cart(*this);
    }

    using value_type = value_t;

    bool valid() const
    {
        return _cart_queue != nullptr && _valid == true;
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

    slotted_cart_queue<value_type> * _cart_queue{nullptr};
};

template <typename value_t>
class slotted_cart_queue
{
    struct cart_slots_t
    {
        using _internal_slot_cart_type = std::vector<value_t>;

        cart_slots_t() = default;
        cart_slots_t(scq::slot_count slot_count, scq::cart_capacity cart_capacity) :
            _cart_capacity{cart_capacity.cart_capacity},
            _internal_cart_slots(slot_count.slot_count)  // default init slot_count many vectors
        {}

        struct slot_cart_t
        {
            scq::slot_id _slot_id;
            _internal_slot_cart_type * _internal_slot_cart_ptr;
            size_t _cart_capacity;

            size_t size()
            {
                return _internal_slot_cart_ptr->size();
            }

            size_t capacity()
            {
                return _cart_capacity;
            }

            bool empty()
            {
                return _internal_slot_cart_ptr->empty();
            }

            bool full()
            {
                return size() >= capacity();
            }

            void emplace_back(value_t value)
            {
                assert(size() < capacity());
                _internal_slot_cart_ptr->emplace_back(std::move(value));
            }
        };

        size_t size()
        {
            return _internal_cart_slots.size();
        }

        slot_cart_t slot(scq::slot_id slot_id)
        {
            _internal_slot_cart_type & slot_cart = _internal_cart_slots[slot_id.slot_id];
            return {slot_id, &slot_cart, _cart_capacity};
        }

        size_t _cart_capacity{};
        std::vector<_internal_slot_cart_type> _internal_cart_slots{}; // position is slot_id
    };

    struct empty_carts_queue_t
    {
        empty_carts_queue_t() = default;
        empty_carts_queue_t(cart_count cart_count) :
            _count{static_cast<std::ptrdiff_t>(cart_count.cart_count)},
            _cart_count{cart_count.cart_count}
        {}

        bool empty()
        {
            return _count == 0;
        }

        void enqueue()
        {
            ++_count;
        }

        void dequeue()
        {
            --_count;
        }

        void _check_invariant()
        {
            assert(0 <= _count);
            assert(_count <= _cart_count);

            if (!(0 <= _count))
                throw std::runtime_error{"empty_carts_queue.count: negative"};

            if (!(_count <= _cart_count))
                throw std::runtime_error{std::string{"empty_carts_queue.count: FULL, _count: "} + std::to_string(_count) + " <= " + std::to_string(_cart_count)};
        }

        std::ptrdiff_t _count{};
        std::size_t _cart_count{};
    };

    struct full_carts_queue_t
    {
        full_carts_queue_t() = default;
        full_carts_queue_t(cart_count cart_count) :
            _count{0},
            _cart_count{cart_count.cart_count}
        {}

        bool empty()
        {
            return _count == 0;
        }

        void enqueue()
        {
            ++_count;
        }

        void dequeue()
        {
            --_count;
        }

        void _check_invariant()
        {
            assert(0 <= _count);
            assert(_count <= _cart_count);

            if (!(0 <= _count))
                throw std::runtime_error{"full_carts_queue.count: negative"};

            if (!(_count <= _cart_count))
                throw std::runtime_error{std::string{"full_carts_queue.count: FULL, _count: "} + std::to_string(_count) + " <= " + std::to_string(_cart_count)};
        }

        std::ptrdiff_t _count{};
        std::size_t _cart_count{};

        using _internal_full_cart_type = std::pair<slot_id, std::vector<value_t>>;
        std::vector<_internal_full_cart_type> _internal_queue{};
    };

public:
    using value_type = value_t;
    using cart_future_type = cart<value_type>;

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

    void enqueue(slot_id slot, value_type value)
    {
        bool full_queue_was_empty{};
        bool queue_was_closed{};

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            queue_was_closed = _queue_closed;

            auto slot_cart = _cart_slots.slot(slot);

            if (!queue_was_closed && slot_cart.empty())
            {
                _empty_cart_queue_empty_or_closed_cv.wait(cart_management_lock, [this, &slot_cart, slot]
                {
                    // we need to refresh the cart in the current slot after wake-up, because it could have changed
                    // since the last invocation
                    slot_cart = _cart_slots.slot(slot);

                    // wait until either an empty cart is ready, or the slot has a cart, or the queue was closed
                    return !_empty_carts_queue.empty() || !slot_cart.empty() || _queue_closed == true;
                });

                queue_was_closed = _queue_closed;

                // if the current slot still has no cart and we have an available empty cart, use that empty cart in
                // this slot
                if (!queue_was_closed && slot_cart.empty())
                {
                    // this assert must be true because of the condition within _empty_cart_queue_empty_or_closed_cv
                    assert(!_empty_carts_queue.empty());

                    _empty_carts_queue.dequeue(); // TODO, we need to do something with returned empty_cart
                    assert_cart_count_variant();
                }
            }

            if (!queue_was_closed)
            {
                slot_cart.emplace_back(std::move(value));

                if (slot_cart.full())
                {
                    full_queue_was_empty = full_slot_cart_queue_is_empty();
                    move_slot_cart_into_full_cart_queue(slot);
                }
            }
        }

        if (full_queue_was_empty)
            _full_cart_queue_empty_or_closed_cv.notify_all();

        if (queue_was_closed)
            throw std::overflow_error{"slotted_cart_queue is already closed."};
    }

    cart_future_type dequeue()
    {
        typename full_carts_queue_t::_internal_full_cart_type _tmp_cart{};

        bool full_queue_was_empty{};

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            _full_cart_queue_empty_or_closed_cv.wait(cart_management_lock, [this]
            {
                // wait until first cart is full
                return !full_slot_cart_queue_is_empty() || _queue_closed == true;
            });

            full_queue_was_empty = full_slot_cart_queue_is_empty();

            if (!full_queue_was_empty)
            {
                _tmp_cart = dequeue_slot_cart_from_full_cart_queue();
            }
        }

        // NOTE: cart memory will be released in notify_processed_cart after cart_future was destroyed

        //
        // prepare return data after critical section
        //

        // NOTE: this also handles full_queue_was_empty; if full_queue_was_empty we return a no_state cart
        // this has a asymmetric behaviour from enqueue as we assume multiple "polling" (dequeue) threads. The queue
        // should be closed after all the data was pushed.
        return create_cart_future(_tmp_cart, full_queue_was_empty);
    }

    void close()
    {
        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            _queue_closed = true;
            move_non_empty_slot_carts_into_full_cart_queue();
        }

        _empty_cart_queue_empty_or_closed_cv.notify_all();
        _full_cart_queue_empty_or_closed_cv.notify_all();
    }

private:
    std::size_t _slot_count{};
    std::size_t _cart_count{};
    std::size_t _cart_capacity{};

    empty_carts_queue_t _empty_carts_queue{scq::cart_count{_cart_count}};
    full_carts_queue_t _full_carts_queue{scq::cart_count{_cart_count}};

    friend cart_future_type;

    void assert_cart_count_variant()
    {
        _empty_carts_queue._check_invariant();
        _full_carts_queue._check_invariant();
    }

    void notify_processed_cart(cart_future_type & cart)
    {
        bool empty_queue_was_empty{};
        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            empty_queue_was_empty = _empty_carts_queue.empty();

            _empty_carts_queue.enqueue(); // TODO add empty cart to queue
            assert_cart_count_variant();
        }

        if (empty_queue_was_empty)
            _empty_cart_queue_empty_or_closed_cv.notify_all();
    }

    void move_non_empty_slot_carts_into_full_cart_queue()
    {
        // TODO: if pending slots are more than queue capacity? is that a problem?

        // put all non-full carts into full queue (no element can't be added any more and all pending elements =
        // active to fill elements must be processed)
        for (size_t slot_id = 0u; slot_id < _cart_slots.size(); ++slot_id)
        {
            auto cart_slot = _cart_slots.slot(scq::slot_id{slot_id});
            if (!cart_slot.empty())
                move_slot_cart_into_full_cart_queue(scq::slot_id{slot_id});
        }
    }

    cart_future_type create_cart_future(typename full_carts_queue_t::_internal_full_cart_type tmp_cart, bool queue_was_empty)
    {
        cart_future_type cart{};
        cart._id = tmp_cart.first;
        cart._cart_span = std::move(tmp_cart.second); // TODO: memory should be owned by the queue not the cart
        cart._valid = !queue_was_empty;
        cart._cart_queue = this;
        return cart;
    }

    typename full_carts_queue_t::_internal_full_cart_type dequeue_slot_cart_from_full_cart_queue()
    {
        _full_carts_queue.dequeue();
        assert_cart_count_variant();

        typename full_carts_queue_t::_internal_full_cart_type tmp = std::move(_full_carts_queue._internal_queue.back());
        _full_carts_queue._internal_queue.pop_back();
        return tmp;
    }

    bool full_slot_cart_queue_is_empty()
    {
        return _full_carts_queue.empty();
    }

    void move_slot_cart_into_full_cart_queue(scq::slot_id slot)
    {
        auto slot_cart = _cart_slots.slot(slot);

        assert(slot_cart.size() > 0); // at least one element
        assert(slot_cart.size() <= slot_cart.capacity()); // at most cart capacity many elements

        _full_carts_queue.enqueue();
        assert_cart_count_variant();

        auto & internal_slot_cart = *slot_cart._internal_slot_cart_ptr;
        _full_carts_queue._internal_queue.emplace_back(slot, std::move(internal_slot_cart));
        internal_slot_cart = {}; // reset slotted cart
    }

    bool _queue_closed{false};

    cart_slots_t _cart_slots{scq::slot_count{_slot_count}, scq::cart_capacity{_cart_capacity}};

    std::mutex _cart_management_mutex;
    std::condition_variable _empty_cart_queue_empty_or_closed_cv;
    std::condition_variable _full_cart_queue_empty_or_closed_cv;
};

} // namespace scq
