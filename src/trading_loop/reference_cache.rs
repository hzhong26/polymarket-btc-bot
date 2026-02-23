use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::price_feeds::PriceAsset;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferenceSource {
    Chainlink,
    AggregatedSpot,
}

#[derive(Debug, Clone)]
pub struct CapturedReference {
    pub price: Decimal,
    pub source: ReferenceSource,
    pub captured_at: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
}

/// Cache key: (PriceAsset, window_start) — NOT condition_id.
/// Multiple markets (5m and 15m) sharing the same asset + window_start
/// reuse one captured reference, avoiding duplicate Chainlink RPC calls.
pub struct ReferencePriceCache {
    cache: HashMap<(PriceAsset, DateTime<Utc>), CapturedReference>,
}

impl Default for ReferencePriceCache {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl ReferencePriceCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, asset: PriceAsset, window_start: DateTime<Utc>) -> Option<&CapturedReference> {
        self.cache.get(&(asset, window_start))
    }

    pub fn insert(&mut self, asset: PriceAsset, window_start: DateTime<Utc>, reference: CapturedReference) {
        self.cache.insert((asset, window_start), reference);
    }

    /// Remove entries when their markets settle (settlement-driven, not fixed timeout).
    pub fn prune_settled(&mut self, keys: &[(PriceAsset, DateTime<Utc>)]) {
        for key in keys {
            self.cache.remove(key);
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut cache = ReferencePriceCache::new();
        let now = Utc::now();
        let reference = CapturedReference {
            price: Decimal::new(98000, 0),
            source: ReferenceSource::Chainlink,
            captured_at: now,
            window_start: now,
        };
        cache.insert(PriceAsset::Btc, now, reference);
        assert!(cache.get(PriceAsset::Btc, now).is_some());
        assert!(cache.get(PriceAsset::Eth, now).is_none());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn prune_settled_removes_entries() {
        let mut cache = ReferencePriceCache::new();
        let now = Utc::now();
        cache.insert(
            PriceAsset::Btc,
            now,
            CapturedReference {
                price: Decimal::new(98000, 0),
                source: ReferenceSource::Chainlink,
                captured_at: now,
                window_start: now,
            },
        );
        cache.insert(
            PriceAsset::Eth,
            now,
            CapturedReference {
                price: Decimal::new(3500, 0),
                source: ReferenceSource::AggregatedSpot,
                captured_at: now,
                window_start: now,
            },
        );
        assert_eq!(cache.len(), 2);

        cache.prune_settled(&[(PriceAsset::Btc, now)]);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(PriceAsset::Btc, now).is_none());
        assert!(cache.get(PriceAsset::Eth, now).is_some());
    }
}
