//! This module provides [lease management](https://cloud.google.com/pubsub/docs/lease-management)
//! functionality for message receivers.
//!
//! Note that acknowledgement deadlines are best-effort only unless you enable
//! [exactly-once delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery).

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Configures the automatic extension of the acknowledgement deadline.
///
/// See <https://cloud.google.com/pubsub/docs/lease-management>.
#[derive(Debug, Clone)]
pub struct LeaseExtensionSetting {
    /// The maximum period for which the Subscription should automatically
    /// extend the ack deadline for each message.
    ///
    /// The Subscription will automatically extend the ack deadline of all
    /// fetched Messages up to the duration specified. Automatic deadline
    /// extension beyond the initial receipt may be disabled by specifying a
    /// duration less than 0.
    pub max_extension: Duration,
    /// The maximum duration by which to extend the ack deadline at a time. The
    /// ack deadline will continue to be extended by up to this duration until
    /// `max_extension` is reached. Setting
    /// `max_extension_period` bounds the maximum amount of time before
    /// a message redelivery in the event the subscriber fails to extend the
    /// deadline.
    ///
    /// It must be between 10s and 600s (inclusive). This configuration can be
    /// disabled by specifying a duration less than (or equal to) 0.
    pub max_extension_period: Option<Duration>,

    /// The the min duration for a single lease extension attempt.
    /// By default the 99th percentile of ack latency is used to determine lease extension
    /// periods but this value can be set to minimize the number of extraneous RPCs sent.
    ///
    /// MinExtensionPeriod must be between 10s and 600s (inclusive). This configuration
    /// can be disabled by specifying a duration less than (or equal to) 0.
    /// Defaults to off but set to 60 seconds if the subscription has exactly-once delivery enabled,
    /// which will be added in a future release.
    pub min_extension_period: Option<Duration>,
}

impl LeaseExtensionSetting {
    const MAX_DURATION_PER_LEASE_EXTENSION: Duration = Duration::from_secs(10 * 60);
    const MIN_DURATION_PER_LEASE_EXTENSION: Duration = Duration::from_secs(10);
    const MIN_DURATION_PER_LEASE_EXTENSION_EXACTLY_ONCE: Duration = Duration::from_secs(60);
    const EXACTLY_ONCE_DELIVERY_RETRY_DEADLINE: Duration = Duration::from_secs(600);

    /// TODO(gwik)
    fn bounded_duration(&self, ack_deadline: Duration, exactly_once: bool) -> Duration {
        // Respect the `max_extension_period`.
        let ack_deadline = if let Some(max_extension) = self.max_extension_period {
            ack_deadline.min(max_extension)
        } else {
            ack_deadline
        };

        // If the user explicitly sets a minExtensionPeriod, respect it.
        if let Some(min_extension) = self.min_extension_period {
            ack_deadline.max(min_extension)
        } else if exactly_once {
            // Higher minimum ack_deadline for subscriptions with
            // exactly-once delivery enabled.
            ack_deadline.max(Self::MIN_DURATION_PER_LEASE_EXTENSION_EXACTLY_ONCE)
        } else {
            // Otherwise, lower bound is min ack extension. This is normally bounded
            // when adding datapoints to the distribution, but this is needed for
            // the initial few calls to ack_deadline.
            ack_deadline.max(Self::MIN_DURATION_PER_LEASE_EXTENSION)
        }
    }

    fn new_deadline(&self, start: Instant) -> Deadline {
        Deadline {
            start,
            max: start + self.max_extension,
        }
    }
}

impl Default for LeaseExtensionSetting {
    fn default() -> Self {
        Self {
            max_extension: Duration::from_secs(60 * 60), // 60 minutes
            max_extension_period: None,
            min_extension_period: None,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct Deadline {
    pub(super) start: Instant,
    pub(super) max: Instant,
    // TODO(gwik)
    // token: CancellactionToken
}

#[derive(Debug, Clone)]
pub(super) struct DeadlinesTracker {
    setting: LeaseExtensionSetting,
    exactly_once: bool,
    deadlines: HashMap<String, Deadline>,
}

impl DeadlinesTracker {
    pub(super) fn new(setting: LeaseExtensionSetting, exactly_once: bool) -> Self {
        Self { setting, exactly_once }
    }

    pub(super) fn register(&mut self, ack_id: String) -> Deadline {
        let start = Instant::now();
        let deadline = self.setting.new_deadline(start);

        self.deadlines.insert(ack_id, deadline);
        deadline
    }

    pub(super) fn done(&mut self, ack_id: &String) -> Option<Deadline> {
        self.deadlines.remove(ack_id)
    }
}
