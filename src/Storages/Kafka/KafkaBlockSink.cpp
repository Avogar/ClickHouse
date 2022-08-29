#include <Storages/Kafka/KafkaSink.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/Kafka/WriteBufferToKafkaProducer.h>

namespace DB
{

KafkaSink::KafkaSink(
    StorageKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
}

void KafkaSink::onStart()
{
    buffer = storage.createWriteBuffer(getHeader());

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    format = FormatFactory::instance().getOutputFormat(storage.getFormatName(), *buffer,
        getHeader(), context,
        [this](const Columns & columns, size_t row)
        {
            buffer->countRow(columns, row);
        },
        format_settings);
}

void KafkaSink::consume(Chunk chunk)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void KafkaSink::onFinish()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void KafkaSink::onException()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void KafkaSink::onCancel()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
    cancelled = true;
}

void KafkaSink::finalize()
{
    if (!buffer || !format)
        return;

    WriteBufferFinalizer buffer_finalizer(*buffer);
    format->finalize();
    buffer->flush();
    buffer_finalizer.finalize();
}


}
