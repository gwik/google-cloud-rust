use crate::statement::ToKind;
use google_cloud_googleapis::spanner::v1::key_range::{EndKeyType, StartKeyType};
use google_cloud_googleapis::spanner::v1::KeyRange as InternalKeyRange;
use google_cloud_googleapis::spanner::v1::KeySet as InternalKeySet;
use prost_types::{value, ListValue, Value};

/// A Key can be either a Cloud Spanner row's primary key or a secondary index
/// key. A Key can be used as:
///
///   - A primary key which uniquely identifies a Cloud Spanner row.
///   - A secondary index key which maps to a set of Cloud Spanner rows indexed under it.
///   - An endpoint of primary key/secondary index ranges; see the KeyRange type.
///
/// Rows that are identified by the Key type are outputs of read operation or
/// targets of delete operation in a mutation. Note that for
/// insert/update/insert_or_update/delete mutation types, although they don't
/// require a primary key explicitly, the column list provided must contain
/// enough columns that can comprise a primary key.
///
/// Keys are easy to construct.  For example, suppose you have a table with a
/// primary key of username and product ID.  To make a key for this table:
/// ```
///	let key = Key::new(vec!["john".to_kind(), 16.to_kind()]);
/// ```
/// See the description of Row and Mutation types for how Go types are mapped to
/// Cloud Spanner types. For convenience, Key type supports a range of Rust
/// types:
///   - i64 and Option<i64> are mapped to Cloud Spanner's INT64 type.
///   - f64 and Option<f64> are mapped to Cloud Spanner's FLOAT64 type.
///   - bool and Option<bool> are mapped to Cloud Spanner's BOOL type.
///   - Vec<u8>, &[u8], Option<Vec<u8>> and Option<&[u8]> is mapped to Cloud Spanner's BYTES type.
///   - String, &str, Option<String>, Option<&str> are mapped to Cloud Spanner's STRING type.
///   - chrono::NaiveDateTime and Option<chrono::NaiveDateTime> are mapped to Cloud Spanner's TIMESTAMP type.
///   - chrono::DateTime and Option<chrono::DateTime> are mapped to Cloud Spanner's DATE type.
///   - google_cloud_spanner::value::CommitTimestamp and Option<google_cloud_spanner::value::CommitTimestamp> are mapped to Cloud Spanner's TIMESTAMP type.
#[derive(Clone)]
pub struct Key {
    pub(crate) values: ListValue,
}

/// / A KeySet defines a collection of Cloud Spanner keys and/or key ranges. All
/// / the keys are expected to be in the same table or index. The keys need not be
/// / sorted in any particular way.
/// /
/// / An individual Key can act as a KeySet, as can a KeyRange. Use the KeySets
/// / function to create a KeySet consisting of multiple Keys and KeyRanges. To
/// / obtain an empty KeySet, call KeySets with no arguments.
/// /
/// / If the same key is specified multiple times in the set (for example if two
/// / ranges, two keys, or a key and a range overlap), the Cloud Spanner backend
/// / behaves as if the key were only specified once.
#[derive(Clone)]
pub struct KeySet {
    pub(crate) inner: InternalKeySet,
}

#[derive(Clone)]
pub enum RangeKind {
    /// ClosedOpen is closed on the left and open on the right: the Start
    /// key is included, the End key is excluded.
    ClosedOpen,

    /// ClosedClosed is closed on the left and the right: both keys are included.
    ClosedClosed,

    /// OpenClosed is open on the left and closed on the right: the Start
    /// key is excluded, the End key is included.
    OpenClosed,

    /// OpenOpen is open on the left and the right: neither key is included.
    OpenOpen,
}

///  A KeyRange represents a range of rows in a table or index.
///
///  A range has a Start key and an End key.  IncludeStart and IncludeEnd
///  indicate whether the Start and End keys are included in the range.
///
///  For example, consider the following table definition:
///
/// 	CREATE TABLE UserEvents (
/// 	  UserName STRING(MAX),
/// 	  EventDate STRING(10),
/// 	) PRIMARY KEY(UserName, EventDate);
///
///  The following keys name rows in this table:
///
///  ```
/// 	let key1 = Key::new(vec!["Bob".to_kind(), "2014-09-23".to_kind()]);
/// 	let key2 = Key::new(vec!["Alfred".to_kind(), "2015-06-12".to_kind()]);
///  ```
///
///  Since the UserEvents table's PRIMARY KEY clause names two columns, each
///  UserEvents key has two elements; the first is the UserName, and the second
///  is the EventDate.
///
///  Key ranges with multiple components are interpreted lexicographically by
///  component using the table or index key's declared sort order. For example,
///  the following range returns all events for user "Bob" that occurred in the
///  year 2015:
///  ```
///  	let range = KeyRange::new(
///         Key::new(vec!["Bob".to_kind(), "2015-01-01".to_kind()]),
///         Key::new(vec!["Bob".to_kind(), "2015-12-31".to_kind()]),
/// 		RangeKind::ClosedClosed
/// 	);
///  ```
///
///  Start and end keys can omit trailing key components. This affects the
///  inclusion and exclusion of rows that exactly match the provided key
///  components: if IncludeStart is true, then rows that exactly match the
///  provided components of the Start key are included; if IncludeStart is false
///  then rows that exactly match are not included.  IncludeEnd and End key
///  behave in the same fashion.
///
///  For example, the following range includes all events for "Bob" that occurred
///  during and after the year 2000:
///  ```
/// 	KeyRange::new(
/// 		Key::new(vec!["Bob".to_kind(), "2000-01-01".to_kind()]),
/// 		Key::one("Bob"),
/// 		RangeKind::ClosedClosed
/// 	);
///  ```
///
///  The next example retrieves all events for "Bob":
///
/// 	Key::one("Bob").to_prefix()
///
///  To retrieve events before the year 2000:
///  ```
/// 	let range = KeyRange::new(
/// 		Key::one("Bob"),
/// 		Key::new(vec!["Bob".to_kind(), "2000-01-01".to_kind()]),
/// 		RangeKind::ClosedOpen
/// 	);
///  ```
///  Key ranges honor column sort order. For example, suppose a table is defined
///  as follows:
///
/// 	CREATE TABLE DescendingSortedTable {
/// 	  Key INT64,
/// 	  ...
/// 	) PRIMARY KEY(Key DESC);
///
///  The following range retrieves all rows with key values between 1 and 100
///  inclusive:
///
///  ```
/// 	let range = KeyRange::new(
///         Key::one(100),
/// 		Key::one(1),
/// 		RangeKind::ClosedClosed,
/// 	);
///  ```
///
///  Note that 100 is passed as the start, and 1 is passed as the end, because
///  Key is a descending column in the schema.
#[derive(Clone)]
pub struct KeyRange {
    /// start specifies the left boundary of the key range;.
    pub(crate) start: Key,

    /// end specifies the right boundary of the key range.
    pub(crate) end: Key,

    /// kind describes whether the boundaries of the key range include
    /// their keys.
    pub kind: RangeKind,
}

/// all_keys returns a KeySet that represents all Keys of a table or a index.
pub fn all_keys() -> KeySet {
    KeySet {
        inner: InternalKeySet {
            keys: vec![],
            ranges: vec![],
            all: true,
        },
    }
}

impl KeyRange {
    pub fn new(start: Key, end: Key, kind: RangeKind) -> KeyRange {
        KeyRange { start, end, kind }
    }
}

impl From<KeyRange> for InternalKeyRange {
    fn from(key_range: KeyRange) -> Self {
        let (start, end) = match key_range.kind {
            RangeKind::ClosedClosed => (
                Some(StartKeyType::StartClosed(key_range.start.values)),
                Some(EndKeyType::EndClosed(key_range.end.values)),
            ),
            RangeKind::ClosedOpen => (
                Some(StartKeyType::StartClosed(key_range.start.values)),
                Some(EndKeyType::EndOpen(key_range.end.values)),
            ),
            RangeKind::OpenClosed => (
                Some(StartKeyType::StartOpen(key_range.start.values)),
                Some(EndKeyType::EndClosed(key_range.end.values)),
            ),
            RangeKind::OpenOpen => (
                Some(StartKeyType::StartOpen(key_range.start.values)),
                Some(EndKeyType::EndOpen(key_range.end.values)),
            ),
        };
        InternalKeyRange {
            start_key_type: start,
            end_key_type: end,
        }
    }
}

impl From<KeyRange> for KeySet {
    fn from(key_range: KeyRange) -> Self {
        KeySet {
            inner: InternalKeySet {
                keys: vec![],
                ranges: vec![key_range.into()],
                all: false,
            },
        }
    }
}

impl Key {
    /// one creates new Key
    /// # Examples
    /// ```
    ///  let key1 = Key::one("a");
    ///  let key2 = Key::one(1);
    /// ```
    pub fn one(value: impl ToKind) -> Key {
        return Key::new(vec![value.to_kind()]);
    }

    /// new creates new Key
    /// # Examples
    /// ```
    /// let multi_key = Key::new(vec!["a".to_kind(), 1.to_kind()]);
    /// ```
    pub fn new(values: Vec<value::Kind>) -> Key {
        Key {
            values: ListValue {
                values: values
                    .into_iter()
                    .map(|x| Value { kind: Some(x) })
                    .collect(),
            },
        }
    }

    /// to_prefix returns a KeyRange for all keys where k is the prefix.
    pub fn to_prefix(&self) -> KeyRange {
        KeyRange::new(self.clone(), self.clone(), RangeKind::ClosedClosed)
    }
}

impl From<Key> for KeySet {
    fn from(key: Key) -> Self {
        KeySet {
            inner: InternalKeySet {
                keys: vec![key.values],
                ranges: vec![],
                all: false,
            },
        }
    }
}

impl From<Vec<Key>> for KeySet {
    fn from(keys: Vec<Key>) -> Self {
        let keys = keys.into_iter().map(|key| key.values).collect();
        KeySet {
            inner: InternalKeySet {
                keys,
                ranges: vec![],
                all: false,
            },
        }
    }
}
