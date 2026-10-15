//! Stream service: lifecycle of the streams (load, create, delete, ...) on top of a storage provider.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use parking_lot::RwLock;
use serde_json::Value;
use uuid::Uuid;

use crate::apierror::ApiError;
use crate::buffering::StreamIngestBuffer;
use crate::config::Config;
use crate::constants;
use crate::error::{Error, Result};
use crate::jq::JqFilter;
use crate::logger::Logger;
use crate::metrics::Metrics;
use crate::storage::{new_storage_provider, StorageProvider};
use crate::stream::{Stream, StreamIterator};
use crate::types::{
    SharedStreamInfo, StreamInfo, StreamIteratorRequest, StreamIteratorUuid, StreamProperties,
    StreamUuid,
};
use crate::{log_error, log_info};

pub struct Service {
    config: Arc<Config>,
    logger: Logger,
    storage: Arc<dyn StorageProvider>,
    streams: RwLock<HashMap<StreamUuid, Arc<Stream>>>,
    /// Serializes the creation and the deletion of streams.
    lifecycle: tokio::sync::Mutex<()>,
    metrics: Arc<Metrics>,
}

impl Service {
    /// Create the storage provider configured in `storage`, then load and start the streams.
    pub async fn new(
        config: Arc<Config>,
        logger: Logger,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Service>> {
        // the storage provider has its own logger
        let storage_logger = Logger::new(&config.storage.logger)
            .map_err(|e| Error::msg(format!("invalid storage logger configuration: {e}")))?;
        let storage = new_storage_provider(&config, storage_logger)?;
        Service::with_storage(config, logger, storage, metrics).await
    }

    /// Create the service on top of an existing storage provider.
    pub async fn with_storage(
        config: Arc<Config>,
        logger: Logger,
        storage: Arc<dyn StorageProvider>,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Service>> {
        storage.init().await.inspect_err(|e| {
            log_error!(logger, "Error while initializing the storage provider", "topic" => "server", "method" => "NewService", "storage" => storage.name(), "error" => e.to_string());
        })?;
        let service = Arc::new(Service {
            config,
            logger,
            storage,
            streams: RwLock::new(HashMap::new()),
            lifecycle: tokio::sync::Mutex::new(()),
            metrics,
        });
        service.load_streams().await.inspect_err(|e| {
            log_error!(service.logger, "Error while loading streams", "topic" => "server", "method" => "NewService", "error" => e.to_string());
        })?;
        log_info!(service.logger, "Stream server started", "topic" => "server", "method" => "NewService", "storage" => service.storage.name(), "streams" => service.streams_count());
        Ok(service)
    }

    pub fn storage(&self) -> &Arc<dyn StorageProvider> {
        &self.storage
    }

    pub fn streams_count(&self) -> usize {
        self.streams.read().len()
    }

    async fn start_stream(&self, info: SharedStreamInfo) -> Result<Arc<Stream>> {
        let uuid = info.read().uuid;
        let mut writer = self.storage.new_stream_writer(&info).await?;
        writer.init().await?;
        writer.open().await?;

        let conf = &self.config.streams;
        let ingest_buffer = StreamIngestBuffer::new(
            uuid,
            Duration::from_secs(conf.bulk_flush_frequency.max(0) as u64),
            conf.bulk_max_size.max(1) as usize,
            writer,
            self.logger.clone(),
        );
        let stream = Stream::new(
            info,
            self.logger.clone(),
            conf.log_verbosity,
            self.metrics.clone(),
        );
        let idle_timeout = (conf.iterator_idle_timeout_in_seconds > 0)
            .then(|| Duration::from_secs(conf.iterator_idle_timeout_in_seconds));
        stream.start(
            ingest_buffer,
            conf.channel_buffer_size.max(1) as usize,
            idle_timeout,
        )?;
        self.streams.write().insert(uuid, stream.clone());
        log_info!(self.logger, "Start stream", "topic" => "stream", "method" => "startStream", "stream.uuid" => uuid);
        Ok(stream)
    }

    async fn load_streams(&self) -> Result<()> {
        let infos = self.storage.load_streams().await?;
        let results = join_all(infos.into_iter().map(|info| self.start_stream(info))).await;
        for result in results {
            result?;
        }
        Ok(())
    }

    /// Create a new stream.
    pub async fn create_stream(&self, properties: StreamProperties) -> Result<Arc<Stream>> {
        let _guard = self.lifecycle.lock().await;
        let max_allowed = self.config.streams.max_allowed_streams;
        if max_allowed > 0 && self.streams_count() as u64 >= max_allowed {
            let err = Error::msg("cannot create stream, limit reached");
            log_error!(self.logger, "Cannot create stream", "topic" => "stream", "method" => "CreateStream", "error" => err.to_string());
            return Err(err);
        }

        let uuid = self.storage.generate_new_stream_uuid();
        log_info!(self.logger, "Create stream", "topic" => "stream", "method" => "CreateStream", "stream.uuid" => uuid);
        let mut info = StreamInfo::new(uuid);
        info.properties = properties;
        let info = info.into_shared();

        self.storage.on_create_stream(&info).await?;
        let stream = match self.start_stream(info).await {
            Ok(stream) => stream,
            Err(e) => {
                // do not keep a stream which cannot be used
                let _ = self.storage.delete_stream(&uuid).await;
                return Err(e);
            }
        };
        self.storage.save_stream_catalog().await?;
        Ok(stream)
    }

    /// Delete a stream and all its records.
    pub async fn delete_stream(&self, stream_uuid: &StreamUuid) -> Result<()> {
        let _guard = self.lifecycle.lock().await;
        let stream = self.get_stream(stream_uuid).ok_or_else(|| {
            let err = Error::not_found("stream not found");
            log_error!(self.logger, "Cannot delete stream", "topic" => "stream", "method" => "DeleteStream", "stream.uuid" => stream_uuid, "error" => err.to_string());
            err
        })?;
        stream.close().await?;
        self.streams.write().remove(stream_uuid);
        self.storage.delete_stream(stream_uuid).await?;
        self.storage.save_stream_catalog().await?;
        log_info!(self.logger, "Stream deleted", "topic" => "stream", "method" => "DeleteStream", "stream.uuid" => stream_uuid);
        Ok(())
    }

    pub fn get_stream(&self, stream_uuid: &StreamUuid) -> Option<Arc<Stream>> {
        self.streams.read().get(stream_uuid).cloned()
    }

    fn sorted_streams(&self) -> Vec<Arc<Stream>> {
        let mut streams: Vec<_> = self.streams.read().values().cloned().collect();
        streams.sort_by_cached_key(|s| (s.shared_info().read().creation_date, s.uuid()));
        streams
    }

    /// Uuids of all the streams (sorted by creation date).
    pub fn get_streams_uuids(&self) -> Vec<StreamUuid> {
        self.sorted_streams().iter().map(|s| s.uuid()).collect()
    }

    /// Streams whose properties match every filter (sorted by creation date).
    pub fn get_streams_filtered(&self, filters: &[&JqFilter]) -> Vec<Arc<Stream>> {
        self.sorted_streams()
            .into_iter()
            .filter(|s| filters.iter().all(|f| s.match_filter_properties(f)))
            .collect()
    }

    /// Replace (`replace` is true) or update the properties of a stream.
    pub async fn change_stream_properties(
        &self,
        stream: &Stream,
        properties: StreamProperties,
        replace: bool,
    ) -> StreamProperties {
        if replace {
            stream.set_properties(properties);
        } else {
            stream.update_properties(properties);
        }
        if let Err(e) = self
            .storage
            .on_update_stream_properties(stream.shared_info())
            .await
        {
            log_error!(self.logger, "Cannot save stream properties", "topic" => "stream", "method" => "ChangeStreamProperties", "stream.uuid" => stream.uuid(), "error" => e.to_string());
        }
        stream.properties()
    }

    /// Create a records iterator for a stream.
    pub async fn create_records_iterator(
        &self,
        stream: &Stream,
        request: &StreamIteratorRequest,
    ) -> Result<StreamIteratorUuid, ApiError> {
        let stream_uuid = stream.uuid();
        let error = |code: i32, details: String| {
            ApiError::bad_request(code, "cannot create stream iterator")
                .details(details)
                .stream(stream_uuid)
        };

        // limit the count of iterators of the stream
        let max_iterators = self.config.streams.max_iterators_per_stream;
        if max_iterators > 0 && stream.iterators_count() as i64 >= max_iterators {
            return Err(error(
                constants::ERROR_CANT_CREATE_RECORDS_ITERATOR,
                "too many iterators opened for this stream".into(),
            ));
        }

        let jq_filter = JqFilter::parse_optional(&request.jq_filter)
            .map_err(|e| error(constants::ERROR_INVALID_CREATE_RECORDS_ITERATOR_REQUEST, e))?;

        let iterator_uuid = Uuid::new_v4();
        let cant_create =
            |e: Error| error(constants::ERROR_CANT_CREATE_RECORDS_ITERATOR, e.to_string());
        let mut handler = self
            .storage
            .new_stream_iterator_handler(&stream_uuid, &iterator_uuid)
            .await
            .map_err(cant_create)?;
        handler.open().await.map_err(cant_create)?;
        handler.seek(request).await.map_err(cant_create)?;

        let iterator = StreamIterator::new(
            stream_uuid,
            iterator_uuid,
            request,
            jq_filter,
            handler,
            stream.written_notifier(),
            self.logger.clone(),
            self.metrics.clone(),
        );
        stream
            .add_iterator(Arc::new(iterator))
            .map_err(cant_create)?;
        Ok(iterator_uuid)
    }

    pub async fn build_index(&self, stream_uuid: &StreamUuid) -> Result<Value> {
        self.storage.build_index(stream_uuid).await
    }

    /// Stop every stream (pending records are written) and the storage provider.
    pub async fn stop(&self) {
        let streams: Vec<_> = self.streams.write().drain().map(|(_, s)| s).collect();
        join_all(streams.iter().map(|s| async move {
            if let Err(e) = s.close().await {
                log_error!(self.logger, "Cannot close stream", "topic" => "stream", "method" => "Stop", "stream.uuid" => s.uuid(), "error" => e.to_string());
            }
        }))
        .await;
        if let Err(e) = self.storage.stop().await {
            log_error!(self.logger, "Cannot stop the storage provider", "topic" => "server", "method" => "Stop", "error" => e.to_string());
        }
    }
}
