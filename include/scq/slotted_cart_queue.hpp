#pragma once

#include <cassert>
#include <condition_variable>
#include <cstddef> // std::size_t
#include <future> // future_error
#include <mutex>
#include <optional>
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
class cart_future
{
public:
    cart_future() = default;
    cart_future(cart_future const &) = delete;
    cart_future(cart_future &&) = default;
    cart_future & operator=(cart_future const &) = delete;
    cart_future & operator=(cart_future &&) = default;
    ~cart_future()
    {
        if (valid())
            _cart_queue->notify_processed_cart(*this);
    }

    using value_type = value_t;

    bool valid() const
    {
        return _cart_queue != nullptr;
    }

    std::pair<scq::slot_id, std::span<value_type>> get()
    {
        if (!valid()) // slotted_cart_queue is already closed and no further elements.
            throw std::future_error{std::future_errc::no_state};

        return {_id, _memory_region};
    }

private:
    template <typename>
    friend class slotted_cart_queue;

    scq::slot_id _id{};
    std::span<value_type> _memory_region{};

    slotted_cart_queue<value_type> * _cart_queue{nullptr};
};

template <typename value_t>
class slotted_cart_queue
{
    struct cart_memory_id
    {
        std::size_t cart_memory_id;
    };
    struct queue_memory_t;
    struct cart_slots_t;
    struct empty_carts_queue_t;
    struct full_carts_queue_t;

public:
    using value_type = value_t;
    using cart_future_type = cart_future<value_type>;

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
        using full_cart_type = typename full_carts_queue_t::full_cart_type;

        bool full_queue_was_empty{};
        bool queue_was_closed{};

        std::optional<full_cart_type> full_cart{};

        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            queue_was_closed = _queue_closed;

            auto slot_cart = _cart_slots.slot(slot);

            if (!queue_was_closed && slot_cart.empty())
            {
                _empty_cart_queue_empty_or_closed_cv.wait(cart_management_lock, [this, &slot_cart]
                {
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

                    std::span<value_t> memory_region = _empty_carts_queue.dequeue();
                    slot_cart.set_memory_region(memory_region);
                    assert_cart_count_variant();
                }
            }

            if (!queue_was_closed)
            {
                slot_cart.emplace_back(std::move(value));

                if (slot_cart.full())
                {
                    full_cart = full_carts_queue_t::move_slot_cart_to_full_cart(slot_cart);
                }
            }
        }

        if (full_cart.has_value())
        {
            std::unique_lock<std::mutex> full_cart_queue_lock(_full_cart_queue_mutex);

            full_queue_was_empty = _full_carts_queue.empty();

            // enqueue later
            _full_carts_queue.enqueue(std::move(*full_cart));
            assert_cart_count_variant();
        }

        if (full_queue_was_empty)
            _full_cart_queue_empty_or_closed_cv.notify_all();

        if (queue_was_closed)
            throw std::overflow_error{"slotted_cart_queue is already closed."};
    }

    cart_future_type dequeue()
    {
        cart_future_type cart_future{};

        {
            std::unique_lock<std::mutex> full_cart_queue_lock(_full_cart_queue_mutex);

            _full_cart_queue_empty_or_closed_cv.wait(full_cart_queue_lock, [this]
            {
                // wait until first cart is full
                return !_full_carts_queue.empty() || _queue_closed == true;
            });

            if (!_full_carts_queue.empty())
            {
                auto full_cart = _full_carts_queue.dequeue();
                cart_future._id = full_cart.first;
                cart_future._memory_region = std::move(full_cart.second);
                cart_future._cart_queue = this;
                assert_cart_count_variant();
            }
        }

        // NOTE: cart memory will be released by notify_processed_cart after cart_future was destroyed
        return cart_future;
    }

    void close()
    {
        {
            // this locks the whole queue
            std::scoped_lock lock(_cart_management_mutex, _full_cart_queue_mutex);

            _queue_closed = true;
            _cart_slots.move_active_carts_into_full_carts_queue(_full_carts_queue);
            assert_cart_count_variant();
        }

        _empty_cart_queue_empty_or_closed_cv.notify_all();
        _full_cart_queue_empty_or_closed_cv.notify_all();
    }

private:
    std::size_t _slot_count{};
    std::size_t _cart_count{};
    std::size_t _cart_capacity{};

    queue_memory_t _queue_memory{scq::cart_count{_cart_count}, scq::cart_capacity{_cart_capacity}};
    empty_carts_queue_t _empty_carts_queue{scq::cart_count{_cart_count}, _queue_memory};
    full_carts_queue_t _full_carts_queue{scq::cart_count{_cart_count}};

    friend cart_future_type;

    void assert_cart_count_variant()
    {
        _empty_carts_queue._check_invariant();
        _full_carts_queue._check_invariant();
    }

    void notify_processed_cart(cart_future_type & cart_future)
    {
        bool empty_queue_was_empty{};
        {
            std::unique_lock<std::mutex> cart_management_lock(_cart_management_mutex);

            empty_queue_was_empty = _empty_carts_queue.empty();

            _empty_carts_queue.enqueue(cart_future._memory_region);
            assert_cart_count_variant();
        }

        if (empty_queue_was_empty)
            _empty_cart_queue_empty_or_closed_cv.notify_all();
    }

    std::atomic_bool _queue_closed{false};

    cart_slots_t _cart_slots{scq::slot_count{_slot_count}, scq::cart_capacity{_cart_capacity}};

    std::mutex _cart_management_mutex;
    std::condition_variable _empty_cart_queue_empty_or_closed_cv;
    std::mutex _full_cart_queue_mutex;
    std::condition_variable _full_cart_queue_empty_or_closed_cv;
};

template <typename value_t>
struct slotted_cart_queue<value_t>::queue_memory_t
{
    queue_memory_t() = default;
    queue_memory_t(scq::cart_count cart_count, scq::cart_capacity cart_capacity) :
        _cart_capacity{cart_capacity.cart_capacity},
        _internal_queue_memory(cart_count.cart_count * cart_capacity.cart_capacity)
    {}

    std::span<value_t> memory_region(cart_memory_id cart_memory_id)
    {
        size_t size = _cart_capacity;
        value_t * begin = _internal_queue_memory.data() + cart_memory_id.cart_memory_id * size;
        return {begin, size};
    }

    std::size_t _cart_capacity{};

    std::vector<value_t> _internal_queue_memory{};
};

template <typename value_t>
struct slotted_cart_queue<value_t>::cart_slots_t
{
    cart_slots_t() = default;
    cart_slots_t(scq::slot_count slot_count, scq::cart_capacity cart_capacity) :
        _cart_capacity{cart_capacity.cart_capacity},
        _internal_cart_slots(slot_count.slot_count) // default init slot_count many vectors
    {}

    struct slot_cart_t
    {
        size_t _slot_id;
        size_t _cart_capacity;
        std::span<value_t> * _memory_region_ptr;

        scq::slot_id slot_id() const
        {
            return {_slot_id};
        }

        size_t size() const
        {
            return memory_region().size();
        }

        size_t capacity() const
        {
            return _cart_capacity;
        }

        bool empty() const
        {
            return memory_region().empty();
        }

        bool full() const
        {
            return size() >= capacity();
        }

        void emplace_back(value_t value)
        {
            assert(size() < capacity());
            std::span<value_t> _memory_region = memory_region();
            _memory_region = std::span<value_t>(_memory_region.data(), _memory_region.size() + 1u);
            _memory_region.back() = std::move(value);
            set_memory_region(_memory_region);
        }

        void set_memory_region(std::span<value_t> memory_region_span)
        {
            assert(_memory_region_ptr != nullptr);
            *_memory_region_ptr = std::span<value_t>{memory_region_span};
        }

        std::span<value_t> memory_region() const
        {
            assert(_memory_region_ptr != nullptr);
            return *_memory_region_ptr;
        }
    };

    slot_cart_t slot(scq::slot_id slot_id)
    {
        std::span<value_t> & memory_region = _internal_cart_slots[slot_id.slot_id];
        return {slot_id.slot_id, _cart_capacity, &memory_region};
    }

    void move_active_carts_into_full_carts_queue(full_carts_queue_t & full_carts_queue)
    {
        // TODO: if pending slots are more than queue capacity? is that a problem?

        // put all non-empty / non-full carts into full queue (no element can't be added any more and all pending
        // elements = active to fill elements must be processed)
        for (size_t slot_id = 0u; slot_id < _internal_cart_slots.size(); ++slot_id)
        {
            auto slot_cart = slot(scq::slot_id{slot_id});
            if (!slot_cart.empty())
            {
                auto full_cart = full_carts_queue_t::move_slot_cart_to_full_cart(slot_cart);
                full_carts_queue.enqueue(full_cart);
                full_carts_queue._check_invariant();
            }
        }
    }

    size_t _cart_capacity{};

    std::vector<std::span<value_t>> _internal_cart_slots{}; // position is slot_id
};

template <typename value_t>
struct slotted_cart_queue<value_t>::empty_carts_queue_t
{
    empty_carts_queue_t() = default;
    empty_carts_queue_t(cart_count cart_count, queue_memory_t & queue_memory) :
        _count{static_cast<std::ptrdiff_t>(cart_count.cart_count)},
        _cart_count{cart_count.cart_count},
        _internal_queue{}
    {
        _internal_queue.reserve(_count);

        for (size_t i = 0; i < _cart_count; ++i)
            _internal_queue.push_back(queue_memory.memory_region(cart_memory_id{i}));
    }

    bool empty()
    {
        return _count == 0;
    }

    void enqueue(std::span<value_t> memory_region)
    {
        _internal_queue.push_back(std::span<value_t>{memory_region.data(), 0});

        ++_count;
    }

    std::span<value_t> dequeue()
    {
        --_count;

        std::span<value_t> memory_region{_internal_queue.back().data(), 0};
        _internal_queue.pop_back();
        return memory_region;
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

    std::vector<std::span<value_t>> _internal_queue{};
};

template <typename value_t>
struct slotted_cart_queue<value_t>::full_carts_queue_t
{
    using full_cart_type = std::pair<slot_id, std::span<value_t>>;
    using slot_cart_type = typename cart_slots_t::slot_cart_t;

    full_carts_queue_t() = default;
    full_carts_queue_t(cart_count cart_count) :
        _count{0},
        _cart_count{cart_count.cart_count}
    {
        _internal_queue.reserve(_cart_count);
    }

    bool empty()
    {
        return _count == 0;
    }

    static full_cart_type move_slot_cart_to_full_cart(slot_cart_type & slot_cart)
    {
        assert(slot_cart.size() > 0); // at least one element
        assert(slot_cart.size() <= slot_cart.capacity()); // at most cart capacity many elements

        full_cart_type full_cart{slot_cart.slot_id(), slot_cart.memory_region()};
        slot_cart.set_memory_region(std::span<value_t>{}); // reset slot
        return full_cart;
    }

    void enqueue(full_cart_type full_cart)
    {
        ++_count;

        _internal_queue.push_back(full_cart);
    }

    full_cart_type dequeue()
    {
        --_count;

        full_cart_type tmp = std::move(_internal_queue.back());
        _internal_queue.pop_back();
        return tmp;
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

    std::vector<full_cart_type> _internal_queue{};
};

} // namespace scq
