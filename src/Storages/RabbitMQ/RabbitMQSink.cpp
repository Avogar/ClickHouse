#include <Storages/RabbitMQ/RabbitMQSink.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/logger_useful.h>


namespace DB
{

RabbitMQSink::RabbitMQSink(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
    storage.unbindExchange();
}


void RabbitMQSink::onStart()
{
    buffer = storage.createWriteBuffer();
    buffer->activateWriting();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    format = FormatFactory::instance().getOutputFormat(storage.getFormatName(), *buffer, getHeader(), context,
        [this](const Columns & /* columns */, size_t /* rows */)
        {
            buffer->countRow();
        },
        format_settings);
}


void RabbitMQSink::consume(Chunk chunk)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}


void RabbitMQSink::onFinish()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void RabbitMQSink::onException()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void RabbitMQSink::onCancel()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
    cancelled = true;
}

void RabbitMQSink::finalize()
{
    if (!format || !buffer)
        return;

    WriteBufferFinalizer buffer_finalizer(*buffer);
    format->finalize();
    buffer->updateMaxWait();
    buffer_finalizer.finalize();
}

}
