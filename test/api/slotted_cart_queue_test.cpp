
#include <gtest/gtest.h>

#include <chrono>
#include <set>
#include <span>
#include <thread>

#include <scq/slotted_cart_queue.hpp>

static constexpr std::chrono::milliseconds wait_time(10);

TEST(slotted_cart_queue_test, default_construct)
{
    scq::slotted_cart_queue<int> queue{};
}

TEST(slotted_cart_queue_test, valid_construct)
{
    scq::slotted_cart_queue<int> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};
}

TEST(slotted_cart_queue_test, invalid_construct)
{
    // capacity of a cart is too small (a cart should be able to store at least one item)
    EXPECT_THROW((scq::slotted_cart_queue<int>{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{0}}),
                 std::logic_error);

    // less carts than slots (would dead-lock)
    EXPECT_THROW((scq::slotted_cart_queue<int>{scq::slot_count{5}, scq::cart_count{1}, scq::cart_capacity{1}}),
                 std::logic_error);
}

TEST(single_item_cart_sequential, single_enqueue_dequeue)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.enqueue(scq::slot_id{1}, value_type{100});

    // item enqueued will be available immediately. (cart can contain only one item)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.first.slot_id, 1);
        EXPECT_EQ(*cart_data.second.begin(), value_type{100});
    }

    queue.enqueue(scq::slot_id{2}, value_type{200});

    // item enqueued will be available immediately. (cart can contain only one item)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        EXPECT_EQ(cart_data.first.slot_id, 2);
        EXPECT_EQ(*cart_data.second.begin(), value_type{200});
    }
}

TEST(single_item_cart_sequential, multiple_enqueue_dequeue)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    using set_key = std::pair<std::size_t, value_type>;
    std::set<set_key> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}}
    };

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101});
    queue.enqueue(scq::slot_id{1}, value_type{102});
    queue.enqueue(scq::slot_id{1}, value_type{103});
    queue.enqueue(scq::slot_id{2}, value_type{200});

    for (int i = 0; i < 5; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        set_key actual_key{std::get<0>(cart_data).slot_id, std::get<1>(cart_data)[0]};
        auto it = expected.find(actual_key);

        // lookup (slot, value) in expected set
        EXPECT_NE(it, expected.end()) << (i + 1) << ". iteration";

        // and erase it to ensure that a value was only dequeued once
        if (it != expected.end())
            expected.erase(it);
    }

    // all results seen
    EXPECT_EQ(expected.size(), 0u);
}

TEST(single_item_cart_concurrent, single_producer_single_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    using set_key = std::pair<std::size_t, value_type>;
    std::set<set_key> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}}
    };

    std::thread enqueue_thread{[&queue]()
    {
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{1}, value_type{100});
        queue.enqueue(scq::slot_id{1}, value_type{101});
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{2}, value_type{200});
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{1}, value_type{103});
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{1}, value_type{102});
    }};

    for (int i = 0; i < 5; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        set_key actual_key{std::get<0>(cart_data).slot_id, std::get<1>(cart_data)[0]};
        auto it = expected.find(actual_key);

        // lookup (slot, value) in expected set
        EXPECT_NE(it, expected.end()) << (i + 1) << ". iteration";

        // and erase it to ensure that a value was only dequeued once
        if (it != expected.end())
            expected.erase(it);
    }

    enqueue_thread.join();

    // all results seen
    EXPECT_EQ(expected.size(), 0u);
}

TEST(single_item_cart_concurrent, single_producer_multiple_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    using set_key = std::pair<std::size_t, value_type>;
    std::set<set_key> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}}
    };

    std::thread enqueue_thread{[&queue]()
    {
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{1}, value_type{100});
        queue.enqueue(scq::slot_id{1}, value_type{101});
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{2}, value_type{200});
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{1}, value_type{103});
        std::this_thread::sleep_for(wait_time);
        queue.enqueue(scq::slot_id{1}, value_type{102});
    }};


    // initialise 5 consuming threads
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        static std::mutex expected_mutex{};

        return std::thread([&queue, &expected]
        {
            scq::cart<value_type> cart = queue.dequeue();
            EXPECT_TRUE(cart.valid());
            std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

            set_key actual_key{std::get<0>(cart_data).slot_id, std::get<1>(cart_data)[0]};

            {
                std::unique_lock lk{expected_mutex};

                auto it = expected.find(actual_key);

                // lookup (slot, value) in expected set
                EXPECT_NE(it, expected.end());

                // and erase it to ensure that a value was only dequeued once
                if (it != expected.end())
                    expected.erase(it);
            }
        });
    });

    enqueue_thread.join();

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    // all results seen
    EXPECT_EQ(expected.size(), 0u);
}

TEST(single_item_cart_concurrent, multiple_producer_single_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    using set_key = std::pair<std::size_t, value_type>;
    std::set<set_key> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}}
    };

    // initialise 5 producing threads
    std::vector<std::thread> enqueue_threads(5);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue]
        {
            std::this_thread::sleep_for(thread_id * wait_time);

            switch (thread_id)
            {
                case 0:
                    queue.enqueue(scq::slot_id{1}, value_type{100});
                    break;
                case 1:
                    queue.enqueue(scq::slot_id{1}, value_type{101});
                    break;
                case 2:
                    queue.enqueue(scq::slot_id{2}, value_type{200});
                    break;
                case 3:
                    queue.enqueue(scq::slot_id{1}, value_type{103});
                    break;
                case 4:
                    queue.enqueue(scq::slot_id{1}, value_type{102});
                    break;
            }
        });
    });

    for (int i = 0; i < 5; ++i)
    {
        scq::cart<value_type> cart = queue.dequeue();
        EXPECT_TRUE(cart.valid());
        std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

        set_key actual_key{std::get<0>(cart_data).slot_id, std::get<1>(cart_data)[0]};
        auto it = expected.find(actual_key);

        // lookup (slot, value) in expected set
        EXPECT_NE(it, expected.end()) << (i + 1) << ". iteration";

        // and erase it to ensure that a value was only dequeued once
        if (it != expected.end())
            expected.erase(it);
    }

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();

    // all results seen
    EXPECT_EQ(expected.size(), 0u);
}

TEST(single_item_cart_concurrent, multiple_producer_multiple_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    // expected set contains all (expected) results; after the test which set should be empty (each matching result will
    // be crossed out)
    using set_key = std::pair<std::size_t, value_type>;
    std::set<set_key> expected
    {
        {1, value_type{100}},
        {1, value_type{101}},
        {1, value_type{102}},
        {1, value_type{103}},
        {2, value_type{200}}
    };

    // initialise 5 producing threads
    std::vector<std::thread> enqueue_threads(5);
    std::generate(enqueue_threads.begin(), enqueue_threads.end(), [&]()
    {
        static size_t thread_id = 0;
        return std::thread([thread_id = thread_id++, &queue]
        {
            std::this_thread::sleep_for(thread_id * wait_time);

            switch (thread_id)
            {
                case 0:
                    queue.enqueue(scq::slot_id{1}, value_type{100});
                    break;
                case 1:
                    queue.enqueue(scq::slot_id{1}, value_type{101});
                    break;
                case 2:
                    queue.enqueue(scq::slot_id{2}, value_type{200});
                    break;
                case 3:
                    queue.enqueue(scq::slot_id{1}, value_type{103});
                    break;
                case 4:
                    queue.enqueue(scq::slot_id{1}, value_type{102});
                    break;
            }
        });
    });

    // initialise 5 consuming threads
    std::vector<std::thread> dequeue_threads(5);
    std::generate(dequeue_threads.begin(), dequeue_threads.end(), [&]()
    {
        static std::mutex expected_mutex{};

        return std::thread([&queue, &expected]
        {
            scq::cart<value_type> cart = queue.dequeue();
            EXPECT_TRUE(cart.valid());
            std::pair<scq::slot_id, std::span<value_type>> cart_data = cart.get();

            set_key actual_key{std::get<0>(cart_data).slot_id, std::get<1>(cart_data)[0]};

            {
                std::unique_lock lk{expected_mutex};

                auto it = expected.find(actual_key);

                // lookup (slot, value) in expected set
                EXPECT_NE(it, expected.end());

                // and erase it to ensure that a value was only dequeued once
                if (it != expected.end())
                    expected.erase(it);
            }
        });
    });

    for (auto && enqueue_thread: enqueue_threads)
        enqueue_thread.join();

    for (auto && dequeue_thread: dequeue_threads)
        dequeue_thread.join();

    // all results seen
    EXPECT_EQ(expected.size(), 0u);
}

TEST(single_item_cart_close_queue, no_producer_no_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.close();
}

TEST(single_item_cart_close_queue, single_producer_no_consumer)
{
    using value_type = int;

    // this slotted_cart_queue should behave like a normal queue, but with nondeterministic results
    scq::slotted_cart_queue<value_type> queue{scq::slot_count{5}, scq::cart_count{5}, scq::cart_capacity{1}};

    queue.enqueue(scq::slot_id{1}, value_type{100});
    queue.enqueue(scq::slot_id{1}, value_type{101});
    queue.enqueue(scq::slot_id{1}, value_type{102});
    queue.enqueue(scq::slot_id{1}, value_type{103});

    queue.close();

    EXPECT_THROW(queue.enqueue(scq::slot_id{2}, value_type{200}), std::overflow_error);
}
