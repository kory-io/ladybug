#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "api_test/private_api_test.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/runtime.h"
#include "storage/checkpointer.h"
#include "storage/index/hash_index.h"
#include "storage/page_allocator.h"
#include "storage/page_manager.h"
#include "storage/storage_manager.h"
#include "storage/table/csr_chunked_node_group.h"
#include "storage/table/csr_node_group.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"
#include "storage/table/rel_table_data.h"
#include "storage/table/string_chunk_data.h"
#include "storage/wal/wal.h"
#include "test_env.h"
#include "transaction/transaction_manager.h"
#include <format>

using namespace lbug::common;
using namespace lbug::testing;
using namespace lbug::transaction;
using namespace lbug::storage;

namespace lbug {
namespace testing {

class FlakyCheckpointer {
public:
    explicit FlakyCheckpointer(TransactionManager::init_checkpointer_func_t initFunc)
        : initFunc(std::move(initFunc)) {}

    void setCheckpointer(main::ClientContext& context) const {
        TransactionManager::Get(context)->initCheckpointerFunc = initFunc;
    }

    static void resetCheckpointer(main::ClientContext& context) {
        TransactionManager::Get(context)->initCheckpointerFunc =
            TransactionManager::initCheckpointer;
    }

private:
    TransactionManager::init_checkpointer_func_t initFunc;
};

class FlakyCheckpointerTest : public PrivateApiTest {
public:
    std::string getInputDir() override { return "empty"; }

    void runFlakyCheckpoint(const FlakyCheckpointer& flakyCheckpointer) {
        conn->query("CALL force_checkpoint_on_close=false;");
        conn->query("CALL auto_checkpoint=false");
        conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");
        for (auto i = 0; i < 5000; i++) {
            conn->query(std::format("CREATE (a:test {{id: {}, name: 'name_{}'}});", i, i));
        }
        auto context = getClientContext(*conn);
        flakyCheckpointer.setCheckpointer(*context);
        auto res = conn->query("CHECKPOINT;");
        ASSERT_FALSE(res->isSuccess());
    }

    void runTest(const FlakyCheckpointer& flakyCheckpointer) {
        runFlakyCheckpoint(flakyCheckpointer);
        createDBAndConn();
        auto res = conn->query("MATCH (a:test) RETURN COUNT(a);");
        ASSERT_TRUE(res->isSuccess());
        ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 5000);
    }
};

class FlakyCheckpointerFailsOnCheckpointStorage final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnCheckpointStorage(main::ClientContext& clientContext)
        : Checkpointer(clientContext) {}

    bool checkpointStorage() override { throw RuntimeException("checkpoint failed."); }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointStorageFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnCheckpointStorage>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class CheckpointRetryAfterFailureTest : public FlakyCheckpointerTest {
public:
    void SetUp() override {
        FlakyCheckpointerTest::SetUp();
        ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
        ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
        ASSERT_TRUE(
            conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);")->isSuccess());
        ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    }

    void insertNodes(int64_t begin, int64_t end) const {
        for (auto i = begin; i < end; i++) {
            auto res =
                conn->query(std::format("CREATE (a:test {{id: {}, name: 'name_{}'}});", i, i));
            ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        }
    }

    // Runs one CHECKPOINT with the given failing checkpointer, then restores the default one.
    template<typename FAILING_CHECKPOINTER>
    void failCheckpointWith() const {
        FlakyCheckpointer flakyCheckpointer([](main::ClientContext& context) {
            return std::make_unique<FAILING_CHECKPOINTER>(context);
        });
        auto context = getClientContext(*conn);
        flakyCheckpointer.setCheckpointer(*context);
        ASSERT_FALSE(conn->query("CHECKPOINT;")->isSuccess());
        FlakyCheckpointer::resetCheckpointer(*context);
    }

    void failCheckpoint() const { failCheckpointWith<FlakyCheckpointerFailsOnCheckpointStorage>(); }

    void checkpoint() const {
        auto res = conn->query("CHECKPOINT;");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    }

    void checkNodes(int64_t expectedCount) const {
        auto res = conn->query("MATCH (a:test) RETURN COUNT(a), MIN(a.id), MAX(a.id);");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        auto tuple = res->getNext();
        ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), expectedCount);
        ASSERT_EQ(tuple->getValue(1)->getValue<int64_t>(), 0);
        ASSERT_EQ(tuple->getValue(2)->getValue<int64_t>(), expectedCount - 1);
    }
};

TEST_F(CheckpointRetryAfterFailureTest, RetrySucceedsWithoutNewWrites) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertNodes(0, 100);
    failCheckpoint();
    checkpoint();
    EXPECT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    createDBAndConn();
    checkNodes(100);
}

TEST_F(CheckpointRetryAfterFailureTest, RetrySucceedsWithNewWrites) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertNodes(0, 100);
    failCheckpoint();
    insertNodes(100, 200);
    checkpoint();
    EXPECT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));
    createDBAndConn();
    checkNodes(200);
}

TEST_F(CheckpointRetryAfterFailureTest, RetryFailsWithNewWritesThenReopen) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertNodes(0, 100);
    failCheckpoint();
    insertNodes(100, 200);
    failCheckpoint();
    // Close without checkpointing, as a crash would.
    createDBAndConn();
    checkNodes(200);
}

class FlakyCheckpointerFailsOnSerialization final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnSerialization(main::ClientContext& context)
        : Checkpointer(context) {}

    void serializeCatalogAndMetadata(DatabaseHeader&, bool) override {
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointSerializeFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnSerialization>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnWritingHeader final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnWritingHeader(main::ClientContext& context)
        : Checkpointer(context) {}

    void writeDatabaseHeader(const DatabaseHeader&) override {
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointWriteHeaderFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnWritingHeader>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnFlushingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnFlushingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool /*walRotated*/) override {
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointFlushingShadowFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnFlushingShadow>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnLoggingCheckpoint final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnLoggingCheckpoint(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool /*walRotated*/) override {
        const auto storageManager = mainStorageManager;
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointLoggingCheckpointFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnLoggingCheckpoint>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

class FlakyCheckpointerFailsOnApplyingShadow final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnApplyingShadow(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        const auto storageManager = mainStorageManager;
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        auto wal = WAL::Get(clientContext);
        if (walRotated) {
            wal->logAndFlushCheckpointToFrozen(&clientContext);
        } else {
            wal->logAndFlushCheckpoint(&clientContext);
        }
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointApplyingShadowFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnApplyingShadow>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

TEST_F(CheckpointRetryAfterFailureTest, RetryAfterFailureWithDurableCheckpointRecord) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertNodes(0, 100);
    failCheckpointWith<FlakyCheckpointerFailsOnApplyingShadow>();
    insertNodes(100, 200);
    // The frozen WAL now holds a durable CHECKPOINT record that only recovery can apply, so a
    // retry must not replace it.
    EXPECT_FALSE(conn->query("CHECKPOINT;")->isSuccess());
    createDBAndConn();
    checkNodes(200);
}

TEST_F(CheckpointRetryAfterFailureTest, FailedRetryKeepsDurableCheckpointRecord) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertNodes(0, 100);
    failCheckpointWith<FlakyCheckpointerFailsOnApplyingShadow>();
    insertNodes(100, 200);
    // A retry that fails in its storage phase must not replace the frozen WAL that holds the
    // first checkpoint's CHECKPOINT record.
    failCheckpoint();
    // Close without checkpointing, as a crash would.
    createDBAndConn();
    checkNodes(200);
}

class FlakyCheckpointerFailsOnClearingFiles final : public Checkpointer {
public:
    explicit FlakyCheckpointerFailsOnClearingFiles(main::ClientContext& context)
        : Checkpointer(context) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        const auto storageManager = mainStorageManager;
        auto& shadowFile = storageManager->getShadowFile();
        shadowFile.flushAll(clientContext);
        auto wal = WAL::Get(clientContext);
        if (walRotated) {
            wal->logAndFlushCheckpointToFrozen(&clientContext);
        } else {
            wal->logAndFlushCheckpoint(&clientContext);
        }
        shadowFile.applyShadowPages(*storageManager, clientContext);
        throw RuntimeException("checkpoint failed.");
    }
};

TEST_F(FlakyCheckpointerTest, RecoverFromCheckpointClearingFilesFailure) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runTest(flakyCheckpointer);
}

// Page allocator that fails the first page allocation. In-place checkpoints do not allocate
// pages, so during the storage phase of a checkpoint that only updates a node group with
// persistent data, the first allocation is the flush of an out-of-place column chunk rewrite and
// the failure lands in the middle of that rewrite.
class FailFirstAllocationPageAllocator final : public PageAllocator {
public:
    FailFirstAllocationPageAllocator(PageAllocator& inner, bool& failed)
        : PageAllocator(inner.getDataFH()), inner{inner}, failed{failed} {}

    PageRange allocatePageRange(page_idx_t numPages) override {
        if (!failed) {
            failed = true;
            throw RuntimeException("checkpoint failed.");
        }
        return inner.allocatePageRange(numPages);
    }
    void freePageRange(PageRange block) override { inner.freePageRange(block); }

private:
    PageAllocator& inner;
    bool& failed;
};

class FlakyCheckpointerFailsDuringOutOfPlaceRewrite final : public Checkpointer {
public:
    FlakyCheckpointerFailsDuringOutOfPlaceRewrite(main::ClientContext& context, bool& failed)
        : Checkpointer(context), failed{failed} {}

    bool checkpointStorage() override {
        for (const auto& target : checkpointTargets) {
            FailFirstAllocationPageAllocator pageAllocator(
                *target.storageManager->getDataFH()->getPageManager(), failed);
            const Transaction snapshotTxn(TransactionType::CHECKPOINT,
                Transaction::DUMMY_TRANSACTION_ID, snapshotTS);
            target.storageManager->checkpoint(&clientContext, *target.catalog, snapshotTxn,
                pageAllocator, tableEpochWatermarksByManager.at(target.storageManager));
        }
        throw RuntimeException("expected the injected page allocation failure to fire.");
    }

private:
    bool& failed;
};

// A checkpoint that fails while a column chunk segment is being rewritten out of place must
// leave that segment as it was, so that the data stays readable and later checkpoints succeed.
TEST_F(FlakyCheckpointerTest, RecoverFromFailureDuringOutOfPlaceCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false;");
    conn->query("CALL auto_checkpoint=false;");
    ASSERT_TRUE(
        conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);")->isSuccess());
    constexpr int64_t numInitialRows = 3000;
    constexpr int64_t numRows = 6000;
    constexpr std::string_view namePrefix = "a longer name so the pages fill up ";
    auto nameOf = [&](int64_t i) { return std::format("{}{}", namePrefix, i); };
    auto insertRows = [&](int64_t start, int64_t end) {
        auto res = conn->query(std::format("UNWIND range({}, {}) AS i CREATE (a:test {{id: i, "
                                           "name: concat('{}', CAST(i AS STRING))}});",
            start, end - 1, namePrefix));
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    };
    auto checkRows = [&]() {
        auto res = conn->query("MATCH (a:test) RETURN COUNT(a), CAST(SUM(a.id) AS INT64), "
                               "CAST(SUM(SIZE(a.name)) AS INT64), MIN(a.name), MAX(a.name);");
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        auto row = res->getNext();
        ASSERT_EQ(row->getValue(0)->getValue<int64_t>(), numRows);
        ASSERT_EQ(row->getValue(1)->getValue<int64_t>(), numRows * (numRows - 1) / 2);
        int64_t totalNameSize = 0;
        for (auto i = 0; i < numRows; i++) {
            totalNameSize += nameOf(i).size();
        }
        ASSERT_EQ(row->getValue(2)->getValue<int64_t>(), totalNameSize);
        ASSERT_EQ(row->getValue(3)->getValue<std::string>(), nameOf(0));
        ASSERT_EQ(row->getValue(4)->getValue<std::string>(), nameOf(999));
    };
    insertRows(0, numInitialRows);
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    // Appending to the persistent node group outgrows the pages of its column chunks, so the
    // next checkpoint has to rewrite them out of place.
    insertRows(numInitialRows, numRows);

    auto context = getClientContext(*conn);
    bool failed = false;
    FlakyCheckpointer flakyCheckpointer([&failed](main::ClientContext& ctx) {
        return std::make_unique<FlakyCheckpointerFailsDuringOutOfPlaceRewrite>(ctx, failed);
    });
    flakyCheckpointer.setCheckpointer(*context);
    auto res = conn->query("CHECKPOINT;");
    ASSERT_FALSE(res->isSuccess());
    ASSERT_TRUE(failed);
    checkRows();

    FlakyCheckpointer defaultCheckpointer(
        [](main::ClientContext& ctx) { return std::make_unique<Checkpointer>(ctx); });
    defaultCheckpointer.setCheckpointer(*context);
    res = conn->query("CHECKPOINT;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    checkRows();
}

// Simulates a situation where a database attempts to replay a shadow file from an older database
// with the same path
TEST_F(FlakyCheckpointerTest, ShadowFileDatabaseIDMismatchExistingDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    std::filesystem::remove(databasePath);

    // Temporarily rename the shadow file and frozen WAL file.
    // With WAL rotation, the active .wal is renamed to .wal.checkpoint during checkpoint,
    // so the frozen WAL is what survives after a failed checkpoint.
    auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    auto frozenWalFilePath = StorageUtils::getCheckpointWALFilePath(databasePath);
    auto tmpShadowFilePath = shadowFilePath + "1";
    auto tmpFrozenWalFilePath = frozenWalFilePath + "1";
    ASSERT_TRUE(std::filesystem::exists(shadowFilePath));
    ASSERT_TRUE(std::filesystem::exists(frozenWalFilePath));
    std::filesystem::rename(shadowFilePath, tmpShadowFilePath);
    std::filesystem::rename(frozenWalFilePath, tmpFrozenWalFilePath);

    // Recreate a new DB with the same path as before
    createDBAndConn();
    conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);");

    // Close the DB
    conn.reset();
    database.reset();

    // Rename the files to the original names
    std::filesystem::rename(tmpShadowFilePath, shadowFilePath);
    std::filesystem::rename(tmpFrozenWalFilePath, frozenWalFilePath);

    // The shadow file replay should now fail
    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

TEST_F(FlakyCheckpointerTest, ShadowFileDatabaseIDMismatchNewDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    std::filesystem::remove(databasePath);

    // The shadow file replay should now fail
    EXPECT_THROW(createDBAndConn(), RuntimeException);
}

TEST_F(FlakyCheckpointerTest, ShadowFileDatabaseIDMismatchCorruptedDB) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    auto initFlakyCheckpointer = [](main::ClientContext& context) {
        return std::make_unique<FlakyCheckpointerFailsOnClearingFiles>(context);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    runFlakyCheckpoint(flakyCheckpointer);

    std::filesystem::remove(databasePath);

    // Create a new DB file and write garbage bytes to it
    std::ofstream ofs(databasePath);
    ofs << "1a1a1a1a1a1a1a1a1a1a";
    ofs.close();

    // The shadow file replay should now fail
    EXPECT_THROW(createDBAndConn(), InternalException);
}

// A checkpoint publishes the new chunk metadata of a node group during its storage phase, but
// in-place updates to existing pages are only written to the shadow file until the shadow pages
// are applied at the end of the checkpoint. Read-only transactions are not blocked by a
// checkpoint, so a scan that starts in between must not observe the new metadata together with
// the old page contents (e.g. appended string dictionary offsets that are not on disk yet).
class CheckpointerWithReadBeforeApplyingShadowPages final : public Checkpointer {
public:
    CheckpointerWithReadBeforeApplyingShadowPages(main::ClientContext& clientContext,
        std::function<void()> readFunc)
        : Checkpointer(clientContext), readFunc(std::move(readFunc)) {}

    void logCheckpointAndApplyShadowPages(bool walRotated) override {
        readFunc();
        Checkpointer::logCheckpointAndApplyShadowPages(walRotated);
    }

private:
    std::function<void()> readFunc;
};

#ifndef __SINGLE_THREADED__
TEST_F(FlakyCheckpointerTest, ReadBeforeShadowPagesAreAppliedSeesCommittedStrings) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false;");
    conn->query("CALL auto_checkpoint=false;");
    ASSERT_TRUE(
        conn->query("CREATE NODE TABLE test(id INT64 PRIMARY KEY, name STRING);")->isSuccess());
    // Strings longer than the inline limit, few enough that the appended values below fit in
    // the pages already allocated to the dictionary, so the checkpoint updates them in place.
    auto getName = [](uint64_t i) { return std::format("persistent string value {:04}", i); };
    auto insertRows = [&](uint64_t start, uint64_t end) {
        for (auto i = start; i < end; i++) {
            auto res =
                conn->query(std::format("CREATE (:test {{id: {}, name: '{}'}});", i, getName(i)));
            ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
        }
    };
    constexpr uint64_t numInitialRows = 20;
    constexpr uint64_t numRows = 25;
    insertRows(0, numInitialRows);
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    insertRows(numInitialRows, numRows);

    // Pages of the persistent `name` column chunk (dictionary offsets, string data and indices).
    // An in-place checkpoint of the column updates these pages through the shadow file, while an
    // out-of-place checkpoint writes new pages and leaves these untouched.
    auto context = getClientContext(*conn);
    auto storageManager = StorageManager::Get(*context);
    const auto tableEntry = catalog::Catalog::Get(*context)->getTableCatalogEntry(
        &DUMMY_CHECKPOINT_TRANSACTION, "test");
    auto& nodeTable = storageManager->getTable(tableEntry->getTableID())->cast<NodeTable>();
    const auto* persistentGroup = nodeTable.getNodeGroup(0)->getChunkedNodeGroup(0);
    ASSERT_EQ(persistentGroup->getResidencyState(), ResidencyState::ON_DISK);
    std::unordered_set<page_idx_t> namePages;
    for (const auto* segment :
        persistentGroup->getColumnChunk(tableEntry->getColumnID("name")).getSegments()) {
        const auto& stringChunk = segment->cast<StringChunkData>();
        for (const auto* chunk :
            std::initializer_list<const ColumnChunkData*>{stringChunk.getIndexColumnChunk(),
                stringChunk.getDictionaryChunk().getOffsetChunk(),
                stringChunk.getDictionaryChunk().getStringDataChunk()}) {
            const auto& metadata = chunk->getMetadata();
            for (auto i = 0u; i < metadata.getNumPages(); i++) {
                namePages.insert(metadata.getStartPageIdx() + i);
            }
        }
    }
    ASSERT_FALSE(namePages.empty());

    bool readRan = false;
    uint64_t numShadowedNamePages = 0;
    std::string readError;
    std::vector<std::string> readNames;
    auto readFunc = [&]() {
        // Make sure the `name` column was checkpointed in place, i.e. its updated pages are still
        // pending in the shadow file; otherwise the read below would not read shadow pages.
        auto& shadowFile = storageManager->getShadowFile();
        for (const auto pageIdx : namePages) {
            if (shadowFile.hasShadowPage(storageManager->getDataFH()->getFileIndex(), pageIdx)) {
                numShadowedNamePages++;
            }
        }
        // Run on a separate thread with its own connection, like a concurrent reader would.
        std::thread reader([&]() {
            auto readConn = std::make_unique<main::Connection>(database.get());
            auto res = readConn->query("MATCH (t:test) RETURN t.name ORDER BY t.id;");
            readRan = true;
            if (!res->isSuccess()) {
                readError = res->getErrorMessage();
                return;
            }
            while (res->hasNext()) {
                readNames.push_back(res->getNext()->getValue(0)->getValue<std::string>());
            }
        });
        reader.join();
    };
    FlakyCheckpointer checkpointer([&](main::ClientContext& clientContext) {
        return std::make_unique<CheckpointerWithReadBeforeApplyingShadowPages>(clientContext,
            readFunc);
    });
    checkpointer.setCheckpointer(*context);
    auto res = conn->query("CHECKPOINT;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_TRUE(readRan);
    ASSERT_GT(numShadowedNamePages, 0u);
    ASSERT_TRUE(readError.empty()) << readError;
    ASSERT_EQ(readNames.size(), numRows);
    for (auto i = 0u; i < numRows; i++) {
        EXPECT_EQ(readNames[i], getName(i));
    }
}

// Readers of other storage structures during the same window: between a checkpoint's in-place
// shadow writes and the application of the shadow pages to the data file.
class CheckpointShadowWindowTest : public FlakyCheckpointerTest {
public:
    void SetUp() override {
        FlakyCheckpointerTest::SetUp();
        ASSERT_TRUE(conn->query("CALL force_checkpoint_on_close=false;")->isSuccess());
        ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    }

    void runQuery(const std::string& query) const {
        auto res = conn->query(query);
        ASSERT_TRUE(res->isSuccess()) << query << ": " << res->getErrorMessage();
    }

    // Runs one CHECKPOINT that calls `readFunc` right before the shadow pages are applied.
    void checkpointWithReadInWindow(const std::function<void()>& readFunc) const {
        auto context = getClientContext(*conn);
        FlakyCheckpointer checkpointer([&](main::ClientContext& clientContext) {
            return std::make_unique<CheckpointerWithReadBeforeApplyingShadowPages>(clientContext,
                readFunc);
        });
        checkpointer.setCheckpointer(*context);
        auto res = conn->query("CHECKPOINT;");
        FlakyCheckpointer::resetCheckpointer(*context);
        ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    }

    // Number of pages of the primary key index of `tableName` (slots and string overflow) that
    // currently have a shadow page, and the total number of such pages.
    std::pair<uint64_t, uint64_t> countShadowedPKIndexPages(const std::string& tableName,
        std::string& breakdown) const {
        auto context = getClientContext(*conn);
        auto storageManager = StorageManager::Get(*context);
        const auto tableEntry = catalog::Catalog::Get(*context)->getTableCatalogEntry(
            &DUMMY_CHECKPOINT_TRANSACTION, tableName);
        auto& nodeTable = storageManager->getTable(tableEntry->getTableID())->cast<NodeTable>();
        auto& shadowFile = storageManager->getShadowFile();
        const auto fileIdx = storageManager->getDataFH()->getFileIndex();
        uint64_t numPages = 0, numShadowed = 0;
        for (const auto& entry : nodeTable.getPKIndex()->getStorageEntries()) {
            if (entry.component != "primary_slots" && entry.component != "overflow_slots" &&
                entry.component != "string_overflow") {
                continue;
            }
            uint64_t numShadowedInEntry = 0;
            for (auto i = 0u; i < entry.pageRange.numPages; i++) {
                numPages++;
                numShadowedInEntry +=
                    shadowFile.hasShadowPage(fileIdx, entry.pageRange.startPageIdx + i);
            }
            if (numShadowedInEntry > 0 && breakdown.find(entry.component) == std::string::npos) {
                breakdown += " " + entry.component;
            }
            numShadowed += numShadowedInEntry;
        }
        return {numPages, numShadowed};
    }

    // Looks up every key through the primary key (`WHERE t.id = <key>`) on a new connection and
    // returns, for each key, the names found.
    std::vector<std::vector<std::string>> lookupKeys(const std::vector<std::string>& keyLiterals,
        std::string& error) const {
        std::vector<std::vector<std::string>> result(keyLiterals.size());
        auto readConn = std::make_unique<main::Connection>(database.get());
        for (auto i = 0u; i < keyLiterals.size(); i++) {
            auto res = readConn->query(
                std::format("MATCH (t:test) WHERE t.id = {} RETURN t.name;", keyLiterals[i]));
            if (!res->isSuccess()) {
                error = res->getErrorMessage();
                return result;
            }
            while (res->hasNext()) {
                result[i].push_back(res->getNext()->getValue(0)->getValue<std::string>());
            }
        }
        return result;
    }

    // Primary key lookups while the checkpoint that merges the index inserts (and applies the
    // deletions) of `numInitialRows..numRows` is in its shadow page window.
    void runPKLookupDuringShadowWindow(const std::string& keyType,
        const std::function<std::string(int64_t)>& keyLiteral, bool reopenAfterFirstCheckpoint) {
        constexpr int64_t numInitialRows = 1000;
        constexpr int64_t numRows = 1200;
        // One deleted key that was checkpointed before and one that was not.
        const std::unordered_set<int64_t> deletedKeys{5, 1100};
        runQuery(std::format("CREATE NODE TABLE test(id {} PRIMARY KEY, name STRING);", keyType));
        auto insertRows = [&](int64_t start, int64_t end) {
            for (auto i = start; i < end; i++) {
                runQuery(
                    std::format("CREATE (:test {{id: {}, name: 'name_{}'}});", keyLiteral(i), i));
            }
        };
        insertRows(0, numInitialRows);
        runQuery("CHECKPOINT;");
        if (reopenAfterFirstCheckpoint) {
            createDBAndConn();
            runQuery("CALL force_checkpoint_on_close=false;");
            runQuery("CALL auto_checkpoint=false;");
        }
        // Few enough new keys that they fit in the slots already on disk, so the checkpoint
        // merges them into existing slot pages in place (through the shadow file).
        insertRows(numInitialRows, numRows);
        for (const auto key : deletedKeys) {
            runQuery(std::format("MATCH (t:test) WHERE t.id = {} DELETE t;", keyLiteral(key)));
        }

        std::vector<std::string> keyLiterals;
        for (auto i = 0; i < numRows; i++) {
            keyLiterals.push_back(keyLiteral(i));
        }
        auto checkLookups = [&](const std::vector<std::vector<std::string>>& found,
                                const std::string& when) {
            std::vector<int64_t> wrongKeys;
            for (auto i = 0; i < numRows; i++) {
                std::vector<std::string> expected;
                if (!deletedKeys.contains(i)) {
                    expected.push_back(std::format("name_{}", i));
                }
                if (found[i] != expected) {
                    wrongKeys.push_back(i);
                }
            }
            std::string sample;
            for (auto i = 0u; i < std::min<size_t>(wrongKeys.size(), 10); i++) {
                sample += std::format(" {}(found {})", wrongKeys[i], found[wrongKeys[i]].size());
            }
            EXPECT_TRUE(wrongKeys.empty())
                << when << ": " << wrongKeys.size() << " of " << numRows
                << " primary key lookups returned wrong results, e.g." << sample;
        };

        bool readRan = false;
        std::pair<uint64_t, uint64_t> shadowedPages;
        std::string shadowedBreakdown;
        std::string readError;
        std::vector<std::vector<std::string>> found;
        checkpointWithReadInWindow([&]() {
            shadowedPages = countShadowedPKIndexPages("test", shadowedBreakdown);
            std::thread reader([&]() {
                found = lookupKeys(keyLiterals, readError);
                readRan = true;
            });
            reader.join();
        });
        ASSERT_TRUE(readRan);
        ASSERT_TRUE(readError.empty()) << readError;
        // The updated index pages are only in the shadow file at this point.
        ASSERT_GT(shadowedPages.second, 0u) << "of " << shadowedPages.first << " index pages";
        checkLookups(found,
            std::format("during the shadow page window ({} of {} index pages shadowed:{})",
                shadowedPages.second, shadowedPages.first, shadowedBreakdown));

        std::string afterError;
        const auto foundAfter = lookupKeys(keyLiterals, afterError);
        ASSERT_TRUE(afterError.empty()) << afterError;
        checkLookups(foundAfter, "after the checkpoint");
    }
};

// Reopens the database after the first checkpoint. Without the reopen, the second checkpoint
// currently updates the existing slot pages in place without shadowing them: a disk array only
// shadows pages up to its last page on disk, which it recomputes from its read header when
// checkpointed in memory, but the primary key index publishes the new read headers of its disk
// arrays only after that. Loading the index from disk sets the last page on disk correctly.
TEST_F(CheckpointShadowWindowTest, Int64PrimaryKeyLookupDuringShadowWindowAfterReopen) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    runPKLookupDuringShadowWindow("INT64", [](int64_t i) { return std::to_string(i); },
        true /*reopenAfterFirstCheckpoint*/);
}

// Without a reopen only the string overflow pages of the new keys are shadowed (see above).
TEST_F(CheckpointShadowWindowTest, StringPrimaryKeyLookupDuringShadowWindow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    // Longer than the inline string limit, so the keys live in the index's overflow file.
    runPKLookupDuringShadowWindow("STRING",
        [](int64_t i) { return std::format("'a primary key longer than inline {:05}'", i); },
        false /*reopenAfterFirstCheckpoint*/);
}

TEST_F(CheckpointShadowWindowTest, StringPrimaryKeyLookupDuringShadowWindowAfterReopen) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    runPKLookupDuringShadowWindow("STRING",
        [](int64_t i) { return std::format("'a primary key longer than inline {:05}'", i); },
        true /*reopenAfterFirstCheckpoint*/);
}

// Pauses a query at the first row it evaluates `pause_scan(x)` on, until released, so that a scan
// can be left in progress while another thread checkpoints.
class ScanPauser {
public:
    static ScanPauser& get() {
        static ScanPauser pauser;
        return pauser;
    }

    void reset() {
        std::lock_guard lck{mtx};
        paused = false;
        released = false;
        numCalls = 0;
    }

    static int64_t pauseScan(int64_t value) {
        auto& pauser = get();
        std::unique_lock lck{pauser.mtx};
        if (pauser.numCalls++ == 0) {
            pauser.paused = true;
            pauser.cv.notify_all();
            pauser.cv.wait_for(lck, std::chrono::seconds(60), [&] { return pauser.released; });
        }
        return value;
    }

    bool waitUntilPaused() {
        std::unique_lock lck{mtx};
        return cv.wait_for(lck, std::chrono::seconds(60), [&] { return paused; });
    }

    void release() {
        std::lock_guard lck{mtx};
        released = true;
        cv.notify_all();
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    bool paused = false;
    bool released = false;
    uint64_t numCalls = 0;
};

class CheckpointRunningRelScanTest : public CheckpointShadowWindowTest {
public:
    static constexpr int64_t numDsts = 6000;

    void SetUp() override {
        CheckpointShadowWindowTest::SetUp();
        runQuery("CREATE NODE TABLE P(id INT64 PRIMARY KEY);");
        runQuery("CREATE REL TABLE K(FROM P TO P, w INT64);");
        runQuery(std::format("UNWIND range(0, {}) AS i CREATE (:P {{id: i}});", numDsts));
        runQuery("CHECKPOINT;");
        ScanPauser::get().reset();
        conn->createScalarFunction("pause_scan", &ScanPauser::pauseScan);
    }

    // All rels come from node 0, so a single bound node has many more rels than fit in one
    // vector and a scan of its list pauses in the middle.
    void insertRels(int64_t start, int64_t end) const {
        runQuery(std::format("MATCH (a:P), (b:P) WHERE a.id = 0 AND b.id >= {} AND b.id < {} "
                             "CREATE (a)-[:K {{w: b.id}}]->(b);",
            start, end));
    }

    static std::string scanQuery(bool pause) {
        // Only properties of the rels and the neighbor IDs stored with them, so that the rel scan
        // and pause_scan run in the same pipeline, one vector at a time.
        return std::format("MATCH (a:P)-[r:K]->(b:P) WHERE a.id = 0 RETURN {}, offset(id(b));",
            pause ? "pause_scan(r.w)" : "r.w");
    }

    // Scans the rels of node 0 on a new single-threaded connection.
    std::vector<int64_t> scanRels(bool pause, std::string& error) const {
        auto readConn = std::make_unique<main::Connection>(database.get());
        readConn->setMaxNumThreadForExec(1);
        auto res = readConn->query(scanQuery(pause));
        std::vector<int64_t> ws;
        if (!res->isSuccess()) {
            error = res->getErrorMessage();
            return ws;
        }
        while (res->hasNext()) {
            auto row = res->getNext();
            const auto w = row->getValue(0)->getValue<int64_t>();
            if (w != row->getValue(1)->getValue<int64_t>()) {
                error = std::format("rel with w {} points to node {}", w,
                    row->getValue(1)->getValue<int64_t>());
            }
            ws.push_back(w);
        }
        std::sort(ws.begin(), ws.end());
        return ws;
    }

    static std::vector<int64_t> expectedRels(int64_t start, int64_t end,
        const std::unordered_set<int64_t>& deleted = {}) {
        std::vector<int64_t> result;
        for (auto i = start; i < end; i++) {
            if (!deleted.contains(i)) {
                result.push_back(i);
            }
        }
        return result;
    }

    static void checkRels(const std::vector<int64_t>& actual, const std::vector<int64_t>& expected,
        const std::string& when) {
        std::vector<int64_t> missing, extra;
        std::set_difference(expected.begin(), expected.end(), actual.begin(), actual.end(),
            std::back_inserter(missing));
        std::set_difference(actual.begin(), actual.end(), expected.begin(), expected.end(),
            std::back_inserter(extra));
        EXPECT_EQ(actual.size(), expected.size()) << when;
        EXPECT_TRUE(missing.empty() && extra.empty())
            << when << ": " << missing.size() << " rels missing (first "
            << (missing.empty() ? -1 : missing.front()) << "), " << extra.size()
            << " unexpected (first " << (extra.empty() ? -1 : extra.front()) << ")";
    }

    // Starts a scan of the rels of node 0, pauses it after its first vector, runs a checkpoint
    // and resumes the scan either inside the checkpoint's shadow page window or after the
    // checkpoint has finished.
    std::vector<int64_t> scanAcrossCheckpoint(bool resumeInShadowWindow, std::string& error) {
        auto& pauser = ScanPauser::get();
        std::vector<int64_t> ws;
        std::thread reader([&]() { ws = scanRels(true /*pause*/, error); });
        if (!pauser.waitUntilPaused()) {
            pauser.release();
            reader.join();
            ADD_FAILURE() << "the scan did not reach pause_scan";
            return ws;
        }
        auto checkpoint = std::async(std::launch::async, [&]() {
            if (!resumeInShadowWindow) {
                auto res = conn->query("CHECKPOINT;");
                return res->isSuccess() ? std::string{} : res->getErrorMessage();
            }
            std::string checkpointError;
            checkpointWithReadInWindow([&]() {
                pauser.release();
                reader.join();
            });
            return checkpointError;
        });
        if (checkpoint.wait_for(std::chrono::seconds(60)) != std::future_status::ready) {
            pauser.release();
            ADD_FAILURE() << "the checkpoint is blocked by the paused read";
        }
        const auto checkpointError = checkpoint.get();
        EXPECT_TRUE(checkpointError.empty()) << checkpointError;
        if (!resumeInShadowWindow) {
            pauser.release();
            reader.join();
        }
        return ws;
    }
};

// The rels are only in memory, so the checkpoint flushes them into a new persistent CSR group
// and clears the in-memory groups while the scan is reading them.
TEST_F(CheckpointRunningRelScanTest, InMemoryRelScanStraddlesCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertRels(1, numDsts);
    std::string error;
    const auto ws = scanAcrossCheckpoint(false /*resumeInShadowWindow*/, error);
    EXPECT_TRUE(error.empty()) << error;
    checkRels(ws, expectedRels(1, numDsts), "scan resumed after the checkpoint");
}

// The rels are persistent, and the checkpoint rewrites the CSR group of node 0 (new rels and
// deletions) while the scan is reading the old one.
TEST_F(CheckpointRunningRelScanTest, PersistentRelScanStraddlesCheckpoint) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertRels(1, 4000);
    runQuery("CHECKPOINT;");
    insertRels(4000, numDsts);
    runQuery("MATCH (a:P)-[r:K]->(b:P) WHERE a.id = 0 AND r.w % 7 = 0 DELETE r;");
    std::unordered_set<int64_t> deleted;
    for (auto i = 7; i < numDsts; i += 7) {
        deleted.insert(i);
    }
    std::string error;
    const auto ws = scanAcrossCheckpoint(false /*resumeInShadowWindow*/, error);
    EXPECT_TRUE(error.empty()) << error;
    checkRels(ws, expectedRels(1, numDsts, deleted), "scan resumed after the checkpoint");
}

TEST_F(CheckpointRunningRelScanTest, PersistentRelScanResumesInShadowWindow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertRels(1, 4000);
    runQuery("CHECKPOINT;");
    insertRels(4000, numDsts);
    runQuery("MATCH (a:P)-[r:K]->(b:P) WHERE a.id = 0 AND r.w % 7 = 0 DELETE r;");
    std::unordered_set<int64_t> deleted;
    for (auto i = 7; i < numDsts; i += 7) {
        deleted.insert(i);
    }
    std::string error;
    const auto ws = scanAcrossCheckpoint(true /*resumeInShadowWindow*/, error);
    EXPECT_TRUE(error.empty()) << error;
    checkRels(ws, expectedRels(1, numDsts, deleted), "scan resumed in the shadow page window");
}

// A rel scan that starts in the shadow page window of a checkpoint that updates the persistent
// CSR group in place.
TEST_F(CheckpointRunningRelScanTest, RelScanStartingInShadowWindow) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    insertRels(1, 4000);
    runQuery("CHECKPOINT;");
    // Pages of the persistent CSR group (data columns and CSR header) before the checkpoint.
    auto context = getClientContext(*conn);
    auto storageManager = StorageManager::Get(*context);
    const auto relGroupEntry = catalog::Catalog::Get(*context)
                                   ->getTableCatalogEntry(&DUMMY_CHECKPOINT_TRANSACTION, "K")
                                   ->ptrCast<catalog::RelGroupCatalogEntry>();
    auto& relTable = storageManager->getTable(relGroupEntry->getSingleRelEntryInfo().oid)
                         ->cast<RelTable>();
    std::unordered_set<page_idx_t> relPages;
    auto addPages = [&](const ColumnChunk& chunk) {
        for (const auto* segment : chunk.getSegments()) {
            const auto& metadata = segment->getMetadata();
            for (auto i = 0u; i < metadata.getNumPages(); i++) {
                relPages.insert(metadata.getStartPageIdx() + i);
            }
        }
    };
    for (const auto direction : {RelDataDirection::FWD, RelDataDirection::BWD}) {
        const auto* nodeGroup = relTable.getDirectedTableData(direction)->getNodeGroup(0);
        ASSERT_NE(nodeGroup, nullptr);
        const auto* persistentGroup =
            nodeGroup->cast<CSRNodeGroup>().getPersistentChunkedGroup();
        ASSERT_NE(persistentGroup, nullptr);
        for (auto i = 0u; i < persistentGroup->getNumColumns(); i++) {
            addPages(persistentGroup->getColumnChunk(i));
        }
        const auto& csrHeader = persistentGroup->cast<ChunkedCSRNodeGroup>().getCSRHeader();
        addPages(*csrHeader.offset);
        addPages(*csrHeader.length);
    }
    ASSERT_FALSE(relPages.empty());
    // Few enough new rels that they fit in the gaps of their CSR regions.
    insertRels(4000, 4010);
    runQuery("MATCH (a:P)-[r:K]->(b:P) WHERE a.id = 0 AND r.w % 7 = 0 DELETE r;");
    std::unordered_set<int64_t> deleted;
    for (auto i = 7; i < 4010; i += 7) {
        deleted.insert(i);
    }

    bool readRan = false;
    uint64_t numShadowedRelPages = 0;
    std::string error;
    std::vector<int64_t> ws;
    checkpointWithReadInWindow([&]() {
        auto& shadowFile = storageManager->getShadowFile();
        for (const auto pageIdx : relPages) {
            numShadowedRelPages +=
                shadowFile.hasShadowPage(storageManager->getDataFH()->getFileIndex(), pageIdx);
        }
        std::thread reader([&]() {
            ws = scanRels(false /*pause*/, error);
            readRan = true;
        });
        reader.join();
    });
    ASSERT_TRUE(readRan);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_GT(numShadowedRelPages, 0u) << "of " << relPages.size() << " persistent rel pages";
    checkRels(ws, expectedRels(1, 4010, deleted), "scan started in the shadow page window");
}
#endif // __SINGLE_THREADED__

// ─────────────────────────────────────────────────────────────────────────────
// ReviewFixesTest
// Targeted regression tests for the three fixes made in response to adsharma's
// review comments on PR #332 (feat: non-blocking concurrent checkpoint).
// ─────────────────────────────────────────────────────────────────────────────
class ReviewFixesTest : public PrivateGraphTest {
protected:
    std::string getInputDir() override { return "empty"; }

    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
    }
};

// Fix #1 – lastTimestamp data race
// ─────────────────────────────────────────────────────────────────────────────
// Before the fix, checkpointNoLock() read `lastTimestamp` without holding
// mtxForSerializingPublicFunctionCalls, which is UB when commit() concurrently
// increments it.  The fix snapshots the value under the mutex.
//
// Observable invariant tested here: a write transaction committed while the
// checkpoint drain is waiting for it must be included in the checkpoint.
// Without the fix the snapshot could see a stale (too-low) lastTimestamp,
// causing the MVCC catalog snapshot to exclude the final committed entry.
// After the fix the snapshot is taken under the mutex *after* the drain, so
// it is guaranteed to reflect all commits that happened before the gate.
TEST_F(ReviewFixesTest, CheckpointDrainWaitsForInFlightWrite) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CALL debug_enable_multi_writes=true;");
    conn->query("CREATE NODE TABLE drain_test(id INT64 PRIMARY KEY);");

    // Pre-load N committed rows so the table is non-trivial.
    const int N = 20;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("CREATE (:drain_test {{id: {}}});", i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // Open a write transaction on a second connection and hold it so the
    // checkpoint drain loop is forced to wait.
    auto conn2 = std::make_unique<lbug::main::Connection>(database.get());
    conn2->query("BEGIN TRANSACTION;");
    auto r2 = conn2->query(std::format("CREATE (:drain_test {{id: {}}});", N));
    ASSERT_TRUE(r2->isSuccess()) << r2->getErrorMessage();

    // Start the checkpoint on a background thread.  It will block at the drain
    // step waiting for conn2's write transaction to leave.
    std::promise<bool> ckptOk;
    auto ckptFuture = ckptOk.get_future();
    std::thread ckptThread([&]() {
        auto r = conn->query("CHECKPOINT;");
        ckptOk.set_value(r->isSuccess());
    });

    // Give the checkpoint thread time to reach the drain phase.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Commit the held write — this unblocks the drain.  The checkpoint must
    // then snapshot lastTimestamp *after* this commit is visible.
    conn2->query("COMMIT;");

    ASSERT_TRUE(ckptFuture.get()) << "CHECKPOINT failed";
    ckptThread.join();

    // conn2 holds a raw pointer to `database`; reset it before createDBAndConn()
    // destroys the database, otherwise the conn2 destructor accesses freed memory.
    r2.reset();
    conn2.reset();

    // On reload only checkpointed data is present (WAL has been rotated/cleared).
    // All N+1 rows must be visible because the final commit occurred before the
    // write gate was acquired and snapshotTS must capture it.
    createDBAndConn();
    auto res = conn->query("MATCH (n:drain_test) RETURN count(n) AS c;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    const auto count = res->getNext()->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(count, N + 1) << "Row committed just before the write gate must survive checkpoint";
}

TEST_F(ReviewFixesTest, CheckpointUsesMatchingCatalogForMainAndDefaultGraph) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;"));
    auto createGraph = conn->query("CREATE GRAPH ratatouille ANY;");
    ASSERT_TRUE(createGraph->isSuccess()) << createGraph->getErrorMessage();

    auto checkpoint = conn->query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint->isSuccess()) << checkpoint->getErrorMessage();
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));

    checkpoint.reset();
    createGraph.reset();
    createDBAndConn();
    auto useGraph = conn->query("USE GRAPH ratatouille;");
    ASSERT_TRUE(useGraph->isSuccess()) << useGraph->getErrorMessage();
    auto showTables = conn->query("CALL SHOW_TABLES() RETURN count(*)");
    ASSERT_TRUE(showTables->isSuccess()) << showTables->getErrorMessage();
    ASSERT_EQ(showTables->getNext()->getValue(0)->getValue<int64_t>(), 2);
    showTables.reset();
    useGraph.reset();

    auto readOnlyConfig = *systemConfig;
    readOnlyConfig.readOnly = true;
    auto graphPath = StorageUtils::getGraphPath(databasePath, "ratatouille");
    conn.reset();
    database.reset();

    auto graphDatabase = std::make_unique<main::Database>(graphPath, readOnlyConfig);
    auto graphConnection = std::make_unique<main::Connection>(graphDatabase.get());
    auto graphTables = graphConnection->query("CALL SHOW_TABLES() RETURN count(*)");
    ASSERT_TRUE(graphTables->isSuccess()) << graphTables->getErrorMessage();
    ASSERT_EQ(graphTables->getNext()->getValue(0)->getValue<int64_t>(), 2);
}

// Fix #2 – remove const_cast from NodeGroup::checkpointInMemOnly and
//            NodeGroup::scanAllInsertedAndVersions
// ─────────────────────────────────────────────────────────────────────────────
// Before the fix, InMemChunkedNodeGroup::flush() and
// ChunkedNodeGroup::scanCommitted() took Transaction*, forcing const_casts at
// the call site.  The parameters are now const Transaction*.
//
// The test verifies end-to-end correctness of the in-memory checkpoint path:
// rows that exist only in RAM at checkpoint time must be flushed to disk and
// survive a database reopen.
TEST_F(ReviewFixesTest, CheckpointInMemOnlyDataIntegrity) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE inmem_test(id INT64 PRIMARY KEY, val STRING);");

    // Insert enough rows to span multiple in-memory node groups so both
    // checkpointInMemOnly (flush) and scanAllInsertedAndVersions paths are hit.
    const int N = 300;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("CREATE (:inmem_test {{id: {}, val: 'v{}'}});", i, i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // All rows are still in-memory (this is the first ever checkpoint).
    {
        auto ckptRes = conn->query("CHECKPOINT;");
        ASSERT_TRUE(ckptRes->isSuccess()) << ckptRes->getErrorMessage();
        // ckptRes destroyed here — before createDBAndConn() resets the database —
        // so the FactorizedTable it holds is freed while the allocator is still alive.
    }

    createDBAndConn();
    auto res = conn->query("MATCH (n:inmem_test) RETURN count(n) AS c;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), N)
        << "In-memory rows must survive checkpoint + reopen";

    // Spot-check a specific row to verify scanCommitted correctness.
    res = conn->query("MATCH (n:inmem_test) WHERE n.id = 42 RETURN n.val;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<std::string>(), "v42");
}

// Fix #3 – guard vacuumColumnIDs under schemaMtx
// ─────────────────────────────────────────────────────────────────────────────
// NodeTable::checkpoint() called tableEntry->vacuumColumnIDs() after the write
// gate was released but without holding schemaMtx.  Concurrent reader threads
// iterate the same column-ID set under schemaMtx, creating a data race.
// The fix wraps vacuumColumnIDs in a unique_lock on schemaMtx.
//
// This stress test runs concurrent MATCH queries while CHECKPOINT (which calls
// vacuumColumnIDs) is in progress.  A crash or wrong count indicates the race
// is still present; without the fix this frequently trips TSAN or produces
// heap corruption under sanitizers.
#ifndef __SINGLE_THREADED__
TEST_F(ReviewFixesTest, ConcurrentReadsDuringCheckpointVacuum) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE vacuum_test(id INT64 PRIMARY KEY, name STRING);");

    const int N = 500;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("CREATE (:vacuum_test {{id: {}, name: 'n{}'}});", i, i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // Reader threads run continuous MATCH count queries.  If vacuumColumnIDs
    // races with schemaMtx iteration the count will be wrong or the process
    // will crash.
    std::atomic<bool> stop{false};
    std::vector<std::string> errors;
    std::mutex errorsMtx;

    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            auto rConn = std::make_unique<lbug::main::Connection>(database.get());
            while (!stop.load(std::memory_order_acquire)) {
                auto r = rConn->query("MATCH (n:vacuum_test) RETURN count(n) AS c;");
                if (!r->isSuccess()) {
                    std::lock_guard<std::mutex> lk{errorsMtx};
                    errors.push_back("query failed: " + r->getErrorMessage());
                    return;
                }
                auto cnt = r->getNext()->getValue(0)->getValue<int64_t>();
                if (cnt != N) {
                    std::lock_guard<std::mutex> lk{errorsMtx};
                    errors.push_back(std::format("wrong count: got {} expected {}", cnt, N));
                    return;
                }
            }
        });
    }

    // Let the readers warm up, then trigger the checkpoint that calls vacuumColumnIDs.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto ckptRes = conn->query("CHECKPOINT;");
    ASSERT_TRUE(ckptRes->isSuccess()) << ckptRes->getErrorMessage();

    stop.store(true, std::memory_order_release);
    for (auto& t : readers) {
        t.join();
    }

    std::lock_guard<std::mutex> lk{errorsMtx};
    EXPECT_TRUE(errors.empty()) << "Reader errors during checkpoint: " << errors.front();
}
#endif // __SINGLE_THREADED__

// Hash index basic recovery
//
// HashIndexLocalStorage has no per-entry timestamps; a correct fix for post-snapshotTS
// "ghost key" consistency requires timestamp-aware snapshotting inside the hash-index
// infrastructure and is tracked as a follow-up (pre-existing Vela limitation).
// This test validates the baseline: all rows committed before CHECKPOINT are recoverable
// via PK lookup after a reload.
TEST_F(ReviewFixesTest, HashIndexBasicRecoveryAfterCheckpoint) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE hicac(id INT64 PRIMARY KEY, val STRING);");

    const int N = 20;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("CREATE (:hicac {{id: {}, val: 'v{}'}});", i, i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    {
        auto r = conn->query("CHECKPOINT;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    createDBAndConn();

    // All rows committed before the checkpoint must be recoverable via PK lookup.
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("MATCH (n:hicac) WHERE n.id = {} RETURN n.val;", i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
        ASSERT_TRUE(r->hasNext()) << "Hash index missing entry for id=" << i;
        EXPECT_EQ(r->getNext()->getValue(0)->getValue<std::string>(), "v" + std::to_string(i));
        EXPECT_FALSE(r->hasNext());
    }
}

// Fix #5 – reclaimTailPagesIfNeeded must not introduce overlap with existing FSM entries
// ─────────────────────────────────────────────────────────────────────────────
// reclaimTailPagesIfNeeded() is called after FSM deserialization during recovery. If it inserts
// the tail directly into freeLists (without merge), it can create overlapping entries.
//
// This test exercises the exact overlap pattern deterministically:
// 1) seed an existing free entry that starts at checkpointNumPages,
// 2) reclaim tail [checkpointNumPages, currentNumPages),
// 3) verify resulting FSM has no overlap.
TEST_F(ReviewFixesTest, ReclaimTailMergesWithDeserializedFSMEntries) {
    if (inMemMode) {
        GTEST_SKIP();
    }

    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE fsm_tail(id INT64 PRIMARY KEY, val STRING);");

    auto* context = getClientContext(*conn);
    auto* storageManager = StorageManager::Get(*context);
    auto* dataFH = storageManager->getDataFH();
    auto* pageManager = dataFH->getPageManager();

    // Ensure we have enough physical pages to craft overlapping ranges.
    dataFH->addNewPages(32);
    const auto currentNumPages = dataFH->getNumPages();
    const auto checkpointNumPages = currentNumPages - 8;

    // Existing (deserialized-equivalent) free entry at tail start.
    pageManager->freeImmediatelyRewritablePageRange(dataFH, PageRange(checkpointNumPages, 2));

    // Recovery-time reclaim path under test.
    pageManager->reclaimTailPagesIfNeeded(checkpointNumPages);

    const auto numEntries = pageManager->getNumFreeEntries();
    const auto freeEntries = pageManager->getFreeEntries(0, numEntries);

    std::vector<std::pair<uint64_t, uint64_t>> entries;
    entries.reserve(freeEntries.size());
    for (const auto& entry : freeEntries) {
        entries.emplace_back(entry.startPageIdx, entry.numPages);
    }
    std::sort(entries.begin(), entries.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });

    for (size_t i = 1; i < entries.size(); ++i) {
        const auto prevEnd = entries[i - 1].first + entries[i - 1].second;
        ASSERT_GE(entries[i].first, prevEnd)
            << "Overlapping FSM entries after tail reclaim: [" << entries[i - 1].first << ", "
            << prevEnd << ") and [" << entries[i].first << ", "
            << (entries[i].first + entries[i].second) << ")";
    }
}

TEST_F(ReviewFixesTest, ExplicitCheckpointLeavesPageManagerCleanAfterSecondaryArtIndex) {
    if (inMemMode) {
        GTEST_SKIP();
    }

    conn->query("CALL auto_checkpoint=false;");
    ASSERT_TRUE(conn->query("CREATE NODE TABLE art_clean(id INT64 PRIMARY KEY, name STRING);")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:art_clean {id: 1, name: 'alice'});")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:art_clean {id: 2, name: 'bob'});")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE ART INDEX art_clean_name_idx FOR (n:art_clean) ON (n.name);")
                    ->isSuccess());

    auto checkpoint = conn->query("CHECKPOINT;");
    ASSERT_TRUE(checkpoint->isSuccess()) << checkpoint->getErrorMessage();

    auto* context = getClientContext(*conn);
    auto* pageManager = StorageManager::Get(*context)->getDataFH()->getPageManager();
    EXPECT_FALSE(pageManager->changedSinceLastCheckpoint());
}

TEST_F(ReviewFixesTest, RecoverSecondaryArtIndexCreatedAfterLastCheckpoint) {
    if (inMemMode) {
        GTEST_SKIP();
    }

    conn->query("CALL auto_checkpoint=false;");
    conn->query("CALL force_checkpoint_on_close=false;");
    ASSERT_TRUE(
        conn->query("CREATE NODE TABLE art_wal(id INT64 PRIMARY KEY, name STRING);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:art_wal {id: 1, name: 'alice'});")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:art_wal {id: 2, name: 'bob'});")->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    ASSERT_TRUE(
        conn->query("CREATE ART INDEX art_wal_name_idx FOR (n:art_wal) ON (n.name);")->isSuccess());

    createDBAndConn();

    auto result = conn->query("MATCH (n:art_wal) WHERE n.name = 'bob' RETURN n.id;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_TRUE(result->hasNext());
    EXPECT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 2);
    EXPECT_FALSE(result->hasNext());
}

TEST_F(ReviewFixesTest, BulkArtIndexUsesBlockingCheckpointInsteadOfPhysicalWAL) {
    if (inMemMode) {
        GTEST_SKIP();
    }

    setenv("LBUG_CREATE_INDEX_WAL_THRESHOLD", "1", 1);
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CALL force_checkpoint_on_close=false;");
    ASSERT_TRUE(
        conn->query("CREATE NODE TABLE art_bulk(id INT64 PRIMARY KEY, name STRING);")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:art_bulk {id: 1, name: 'alice'});")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:art_bulk {id: 2, name: 'bob'});")->isSuccess());

    auto createIndex =
        conn->query("CREATE ART INDEX art_bulk_name_idx FOR (n:art_bulk) ON (n.name);");
    unsetenv("LBUG_CREATE_INDEX_WAL_THRESHOLD");
    ASSERT_TRUE(createIndex->isSuccess()) << createIndex->getErrorMessage();
    ASSERT_TRUE(createIndex->hasNext());
    EXPECT_EQ(createIndex->getNext()->getValue(0)->getValue<std::string>(),
        "Index art_bulk_name_idx has been created.");
    EXPECT_FALSE(createIndex->hasNext());

    createDBAndConn();

    auto result = conn->query("MATCH (n:art_bulk) WHERE n.name = 'bob' RETURN n.id;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_TRUE(result->hasNext());
    EXPECT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 2);
    EXPECT_FALSE(result->hasNext());
}

// Fix #4 – defer destructive column move until after nodeGroups->checkpoint()
// ─────────────────────────────────────────────────────────────────────────────
// NodeTable::checkpoint() used to move columns (vacuuming dropped column IDs)
// BEFORE calling nodeGroups->checkpoint(). If nodeGroups->checkpoint() threw,
// the columns vector was already shrunk but the catalog's column IDs were not
// vacuumed, leaving the table in an inconsistent state.  A subsequent retry
// (e.g., from Database::~Database forceCheckpointOnClose) would index out of
// bounds and crash with a segfault.
//
// The fix defers the destructive column move until AFTER nodeGroups->checkpoint()
// succeeds.  These tests exercise the affected code path with ALTER TABLE
// ADD/DROP COLUMN, which triggers column-ID vacuum during checkpoint.

TEST_F(ReviewFixesTest, CheckpointRecoveryAfterAddColumn) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE ckpt_add(id INT64 PRIMARY KEY, name STRING);");

    const int N = 100;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("CREATE (:ckpt_add {{id: {}, name: 'n{}'}});", i, i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // Add a new column — this creates a new column ID that must be vacuumed at checkpoint.
    {
        auto r = conn->query("ALTER TABLE ckpt_add ADD extra INT64 DEFAULT 0;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
        // Set the new column on some rows.
        r = conn->query("MATCH (n:ckpt_add) WHERE n.id < 10 SET n.extra = n.id * 10;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
        r = conn->query("CHECKPOINT;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
        // r destroyed here — before createDBAndConn() resets the database —
        // so the FactorizedTable it holds is freed while the allocator is still alive.
    }

    createDBAndConn();

    // Verify row count.
    auto res = conn->query("MATCH (n:ckpt_add) RETURN count(n) AS c;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), N);

    // Verify the new column exists and has correct values.
    res = conn->query("MATCH (n:ckpt_add) WHERE n.id = 5 RETURN n.extra;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 50);

    res = conn->query("MATCH (n:ckpt_add) WHERE n.id = 50 RETURN n.extra;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 0);
}

TEST_F(ReviewFixesTest, CheckpointRecoveryAfterAddAndDropColumn) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE ckpt_adddrop(id INT64 PRIMARY KEY, name STRING, age INT64);");

    const int N = 100;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(
            std::format("CREATE (:ckpt_adddrop {{id: {}, name: 'n{}', age: {}}});", i, i, 20 + i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // Add a column, then drop one — exercises both add and remove in the column-ID vacuum.
    {
        auto r = conn->query("ALTER TABLE ckpt_adddrop ADD extra STRING DEFAULT 'hello';");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
        r = conn->query("ALTER TABLE ckpt_adddrop DROP name;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
        r = conn->query("CHECKPOINT;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    createDBAndConn();

    // Verify row count.
    auto res = conn->query("MATCH (n:ckpt_adddrop) RETURN count(n) AS c;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), N);

    // Verify dropped column is gone.
    res = conn->query("MATCH (n:ckpt_adddrop) WHERE n.id = 0 RETURN n.name;");
    ASSERT_FALSE(res->isSuccess());

    // Verify remaining columns are correct.
    res = conn->query("MATCH (n:ckpt_adddrop) WHERE n.id = 0 RETURN n.age, n.extra;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    auto row = res->getNext();
    ASSERT_EQ(row->getValue(0)->getValue<int64_t>(), 20);
    ASSERT_EQ(row->getValue(1)->getValue<std::string>(), "hello");
}

TEST_F(ReviewFixesTest, RecoverFromFailedCheckpointAfterAddColumn) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }
    conn->query("CALL force_checkpoint_on_close=false;");
    conn->query("CALL auto_checkpoint=false;");
    conn->query("CREATE NODE TABLE ckpt_fail(id INT64 PRIMARY KEY, name STRING);");

    const int N = 100;
    for (int i = 0; i < N; ++i) {
        auto r = conn->query(std::format("CREATE (:ckpt_fail {{id: {}, name: 'n{}'}});", i, i));
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // Add a column so the checkpoint path must vacuum column IDs.
    {
        auto r = conn->query("ALTER TABLE ckpt_fail ADD extra INT64 DEFAULT 42;");
        ASSERT_TRUE(r->isSuccess()) << r->getErrorMessage();
    }

    // Inject a failure at checkpointStorage level.
    auto context = getClientContext(*conn);
    auto initFlakyCheckpointer = [](main::ClientContext& ctx) {
        return std::make_unique<FlakyCheckpointerFailsOnCheckpointStorage>(ctx);
    };
    FlakyCheckpointer flakyCheckpointer(initFlakyCheckpointer);
    flakyCheckpointer.setCheckpointer(*context);

    // First checkpoint fails.
    auto ckptRes = conn->query("CHECKPOINT;");
    ASSERT_FALSE(ckptRes->isSuccess());

    // Reopen the database — WAL replay + fresh checkpoint must succeed.
    // Before the fix, if a failure happened inside NodeTable::checkpoint() (between the
    // column move and vacuumColumnIDs), the retry checkpoint in ~Database would crash.
    createDBAndConn();

    // Verify data survives WAL replay.
    auto res = conn->query("MATCH (n:ckpt_fail) RETURN count(n) AS c;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), N);

    // Verify the added column is present with its default value.
    res = conn->query("MATCH (n:ckpt_fail) WHERE n.id = 0 RETURN n.extra;");
    ASSERT_TRUE(res->isSuccess()) << res->getErrorMessage();
    ASSERT_EQ(res->getNext()->getValue(0)->getValue<int64_t>(), 42);
}

// ─────────────────────────────────────────────────────────────────────────────
// Regression test for: subgraph catalog writes lost after checkpoint + reopen
// when the main database has pre-existing tables.
//
// Bug: DatabaseManager::createGraph() set defaultGraph to the newly created
// graph before the transaction committed, causing Catalog::Get() in
// Transaction::publishCommit() to return the graph's catalog instead of the
// main catalog.  The main catalog's version was never incremented, so
// CHECKPOINT skipped serializing it (changedSinceLastCheckpoint() was false)
// and the graph entry in the main catalog's `graphs` set was lost on reopen.
//
// This only manifested when the database already had tables (a valid
// catalogPageRange from a previous checkpoint), because with a bare database
// the first-time-serialization path (INVALID_PAGE_IDX) rescued the data.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ReviewFixesTest, SubgraphCatalogPersistsAfterCheckpointWithPreExistingTables) {
    if (inMemMode || systemConfig->checkpointThreshold == 0) {
        GTEST_SKIP();
    }

    // ---- Phase 1: create pre-existing tables (like Hyper-Extract does) ----
    ASSERT_TRUE(conn->query("CALL auto_checkpoint=false;")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE IF NOT EXISTS Template ("
                            "name STRING, domain STRING, type STRING, tags STRING, "
                            "description STRING, language STRING, output STRING, "
                            "guideline STRING, identifiers STRING, opts STRING, display STRING, "
                            "PRIMARY KEY (name));")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE IF NOT EXISTS KnowledgeAbstract ("
                            "id STRING, template_name STRING, lang STRING, type STRING, "
                            "created_at STRING, updated_at STRING, metadata STRING, "
                            "PRIMARY KEY (id));")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE IF NOT EXISTS Entity ("
                            "id STRING, ka_id STRING, entity_type STRING, data STRING, "
                            "PRIMARY KEY (id));")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE REL TABLE IF NOT EXISTS Relates ("
                            "FROM Entity TO Entity, ka_id STRING, "
                            "relation_type STRING, data STRING"
                            ");")
                    ->isSuccess());

    // First checkpoint: persist the schema so the data file has a valid
    // catalogPageRange.  Without this the bug would be masked (the first
    // checkpoint after graph creation would serialize unconditionally).
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));

    // ---- Phase 2: create a subgraph and checkpoint ----
    ASSERT_TRUE(conn->query("CREATE GRAPH regression_test;")->isSuccess());
    ASSERT_TRUE(conn->query("CHECKPOINT;")->isSuccess());
    ASSERT_FALSE(std::filesystem::exists(StorageUtils::getCheckpointWALFilePath(databasePath)));

    // ---- Phase 3: reopen the database ----
    auto graphName = conn->query("CALL show_graphs() RETURN name ORDER BY name;");
    ASSERT_TRUE(graphName->isSuccess()) << graphName->getErrorMessage();
    ASSERT_TRUE(graphName->hasNext());
    // The subgraph must be visible in the current process already. Every node table is also
    // backed by a subgraph, so the list carries the three node tables plus the created graph.
    std::vector<std::string> graphsBeforeReopen;
    while (graphName->hasNext()) {
        graphsBeforeReopen.push_back(graphName->getNext()->getValue(0)->getValue<std::string>());
    }
    ASSERT_EQ(graphsBeforeReopen.size(), 4);
    ASSERT_EQ(graphsBeforeReopen[0], "Entity");
    ASSERT_EQ(graphsBeforeReopen[1], "KnowledgeAbstract");
    ASSERT_EQ(graphsBeforeReopen[2], "Template");
    ASSERT_EQ(graphsBeforeReopen[3], "regression_test");
    graphName.reset();

    // Reopen the database in a fresh connection (simulates a new process).
    createDBAndConn();

    // ---- Phase 4: verify the subgraph survived ----
    auto result = conn->query("CALL show_graphs() RETURN name ORDER BY name;");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    std::vector<std::string> graphs;
    while (result->hasNext()) {
        graphs.push_back(result->getNext()->getValue(0)->getValue<std::string>());
    }

    // The list should include the subgraph created above.
    bool found = false;
    for (const auto& g : graphs) {
        if (g == "regression_test") {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Subgraph 'regression_test' was lost after checkpoint + reopen. "
                       << "Visible graphs count: " << graphs.size();

    // Also verify we can USE GRAPH and see at least the main database tables
    // (proving the main catalog itself was correctly restored).
    ASSERT_TRUE(conn->query("USE GRAPH regression_test;")->isSuccess());
    auto tables = conn->query("CALL show_tables() RETURN name ORDER BY name;");
    ASSERT_TRUE(tables->isSuccess()) << tables->getErrorMessage();
    // Clean up
    ASSERT_TRUE(conn->query("USE GRAPH main;")->isSuccess());
}

} // namespace testing
} // namespace lbug
