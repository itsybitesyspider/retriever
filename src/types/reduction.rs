use crate::internal::mr::reduce::*;
use crate::internal::mr::rvec::RVec;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::storage::Storage;
use std::collections::HashMap;

/// Summarize a `Storage` using a cached multi-layered reduction strategy.
/// Repeated evaluations will only re-compute the parts of the reduction that have changed.
/// If you've used map-reduce in something like CouchDB, this is a lot like that.
///
/// # Type Parameters
///
/// * `ChunkKey`: matches the `ChunkKey` of the `Storage`.
/// * `Element`: matches the `Element` of the `Storage`.
/// * `Summary`: this is the type of the result of summarizing all of the `Elements` in `Storage`.
pub struct Reduction<ChunkKey, Element, Summary> {
    parent_id: u64,
    group_size: usize,
    gc_chunk_list: RVec<Option<ChunkKey>>,
    rules: ReduceRules<Element, Summary>,
    chunkwise_reductions:
        HashMap<ChunkKey, Reduce<Element, Summary>, crate::internal::hasher::HasherImpl>,
    chunkwise_summaries: RVec<Summary>,
    reduction: Reduce<Summary, Summary>,
}

impl<ChunkKey, Element, Summary> Reduction<ChunkKey, Element, Summary>
where
    ChunkKey: ValidKey,
    Summary: Default + Clone,
{
    /// Create a new Reduction on a storage.
    ///
    /// A reduction is constructed from two rules: `Map` and `Fold` (or reduce). The only
    /// difference between these rules is that the `Map` rule examines a single element while
    /// the `Fold` rule examines a list of summaries. Both rules receive a view of the old
    /// summary (which may just be `Default::default()`, if the summary has never been
    /// been computed before.
    ///
    /// Each rule constructs a new summary, and if the new summary is different from the old
    /// summary, returns `Some(new_summary)`. If the summary is unchanged, indicate this by
    /// returning `None`.
    ///
    /// Try to re-use Reductions as much as possible. If you drop a Reduction and re-create it,
    /// then the Reduction's internal index has to be rebuilt, which might take a lot of time.
    ///
    /// # Type Parameters
    ///
    /// * `I`: this is the `ItemKey` matching the `Storage`.
    /// * `Map`: this operation produces a `Summary` of a single `Element`. If the result `Summary`
    ///   has not changed since the last `Summary`, return `None`.
    /// * `Fold`: this operations folds several `Summaries` into one `Summary`. If the result
    ///   `Summary` has not changed since the last `Summary`, return `None`.
    pub fn new<I, Map, Fold>(
        storage: &Storage<ChunkKey, I, Element>,
        group_size: usize,
        map: Map,
        fold: Fold,
    ) -> Self
    where
        I: ValidKey,
        Element: Record<ChunkKey, I>,
        Map: Fn(&Element, &Summary) -> Option<Summary> + Clone + 'static,
        Fold: Fn(&[Summary], &Summary) -> Option<Summary> + Clone + 'static,
    {
        let chunkwise_summaries = RVec::default();
        let reduction = Reduce::new(
            &chunkwise_summaries,
            group_size,
            Self::reduction_rules(map.clone(), fold.clone()),
        );
        Reduction {
            parent_id: storage.id(),
            group_size,
            gc_chunk_list: RVec::default(),
            rules: Self::chunkwise_rules(map.clone(), fold.clone()),
            chunkwise_reductions: HashMap::with_hasher(
                crate::internal::hasher::HasherImpl::default(),
            ),
            chunkwise_summaries,
            reduction,
        }
    }

    fn reduction_rules<Map, Reduce>(_map: Map, reduce: Reduce) -> ReduceRules<Summary, Summary>
    where
        Map: Fn(&Element, &Summary) -> Option<Summary> + Clone + 'static,
        Reduce: Fn(&[Summary], &Summary) -> Option<Summary> + Clone + 'static,
    {
        let map = reduce.clone();
        ReduceRules::new(move |ss, s, _| map(std::slice::from_ref(ss), s), reduce)
    }

    fn chunkwise_rules<Map, Reduce>(map: Map, reduce: Reduce) -> ReduceRules<Element, Summary>
    where
        Map: Fn(&Element, &Summary) -> Option<Summary> + Clone + 'static,
        Reduce: Fn(&[Summary], &Summary) -> Option<Summary> + Clone + 'static,
    {
        ReduceRules::new(move |e, s, _| map(e, s), reduce)
    }

    fn gc<ItemKey>(&mut self, parent: &Storage<ChunkKey, ItemKey, Element>)
    where
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        parent.gc(&mut self.gc_chunk_list, &mut self.chunkwise_reductions);
    }

    /// Reduce all of the elements of the given Storage down to a single value.
    ///
    /// # Example
    ///
    /// ```
    /// use retriever::prelude::*;
    /// use std::borrow::Cow;
    /// use std::collections::HashSet;
    ///
    /// #[derive(Clone, Eq, PartialEq)]
    /// struct Notification {
    ///   user_id: usize,
    ///   id: usize,
    ///   priority: usize,
    ///   msg: String,
    ///   service: &'static str,
    /// }
    ///
    /// #[derive(Clone,Default,Eq,PartialEq)]
    /// struct NotificationSummary {
    ///   count: usize,
    ///   priority: usize,
    ///   services: HashSet<&'static str>,
    /// }
    ///
    /// impl Record<usize, usize> for Notification {
    ///   fn chunk_key(&self) -> Cow<usize> {
    ///     Cow::Owned(self.user_id)
    ///   }
    ///
    ///   fn item_key(&self) -> Cow<usize> {
    ///     Cow::Owned(self.id)
    ///   }
    /// }
    ///
    /// impl NotificationSummary {
    ///   fn add_one(&mut self, n: &Notification) {
    ///     self.count += 1;
    ///     self.priority = self.priority.max(n.priority);
    ///     self.services.insert(n.service);
    ///   }
    ///
    ///   fn add_summary(&mut self, s: &NotificationSummary) {
    ///     self.count += s.count;
    ///     self.priority = self.priority.max(s.priority);
    ///
    ///     for i in s.services.iter() {
    ///       self.services.insert(*i);
    ///     }
    ///   }
    /// }
    ///
    /// let mut storage : Storage<usize, usize, Notification> = Storage::new();
    /// let mut reduction : Reduction<usize, Notification, NotificationSummary> =
    ///   Reduction::new(
    ///     &storage,
    ///     2,
    ///     |element: &Notification, was: &NotificationSummary| {
    ///       let mut summary = NotificationSummary::default();
    ///       summary.add_one(element);
    ///       if &summary != was {
    ///         Some(summary)
    ///       } else {
    ///         None
    ///       }
    ///     },
    ///     |notifications: &[NotificationSummary], was: &NotificationSummary| {
    ///       let mut summary = NotificationSummary::default();
    ///
    ///       for n in notifications {
    ///         summary.add_summary(n);
    ///       }
    ///
    ///       if &summary != was {
    ///         Some(summary)
    ///       } else {
    ///         None
    ///       }
    ///     }
    ///   );
    ///
    /// storage.add(Notification {
    ///   user_id: 1000,
    ///   id: 1,
    ///   priority: 2,
    ///   msg: String::from("You have mail!"),
    ///   service: "mail",
    /// });
    ///
    /// storage.add(Notification {
    ///   user_id: 1000,
    ///   id: 2,
    ///   priority: 2,
    ///   msg: String::from("New email from Susan."),
    ///   service: "mail",
    /// });
    ///
    /// storage.add(Notification {
    ///   user_id: 0,
    ///   id: 3,
    ///   priority: 8,
    ///   msg: String::from("Battery low"),
    ///   service: "power",
    /// });
    ///
    /// storage.add(Notification {
    ///   user_id: 1000,
    ///   id: 4,
    ///   priority: 0,
    ///   msg: String::from("You have won 13 gold coins!!"),
    ///   service: "games",
    /// });
    ///
    /// let summary = reduction.reduce(&storage).unwrap();
    ///
    /// assert_eq!(summary.count, 4);
    /// assert_eq!(summary.priority, 8);
    /// assert!(summary.services.contains("power"));
    /// assert!(summary.services.contains("mail"));
    /// assert!(summary.services.contains("games"));
    ///```
    pub fn reduce<I>(&mut self, storage: &Storage<ChunkKey, I, Element>) -> Option<&Summary>
    where
        Element: Record<ChunkKey, I>,
        I: ValidKey,
    {
        assert_eq!(
            self.parent_id,
            storage.id(),
            "Id mismatch: a Reduction may only be used with it's parent Storage, never any other Storage"
        );

        self.gc(storage);

        let chunkwise_reductions = &mut self.chunkwise_reductions;
        let chunkwise_summaries = &mut self.chunkwise_summaries;
        let group_size = self.group_size;
        let rules = &self.rules;

        chunkwise_summaries.reduce(&self.gc_chunk_list, 1, |chunk_key, _old_summary, idx| {
            assert!(chunk_key.len() <= 1);

            if chunk_key.is_empty() {
                return None;
            }

            let chunk_key = chunk_key[0]
                .as_ref()
                .cloned()
                .expect("retriever bug: chunk keys should be defined for all indices after gc");
            let internal_storage = storage.internal_rvec()[idx].internal_rvec();

            chunkwise_reductions
                .entry(chunk_key)
                .or_insert_with(|| Reduce::new(internal_storage, group_size, rules.clone()))
                .update(&internal_storage)
                .cloned()
        });

        self.reduction.update(&self.chunkwise_summaries)
    }

    /// Reduce all of the elements of a single chunk down to a single value.
    pub fn reduce_chunk<I>(
        &mut self,
        storage: &Storage<ChunkKey, I, Element>,
        chunk_key: &ChunkKey,
    ) -> Option<&Summary>
    where
        Element: Record<ChunkKey, I>,
        I: ValidKey,
    {
        assert_eq!(
            self.parent_id,
            storage.id(),
            "Id mismatch: a Reduction may only be used with it's parent Storage, never any other Storage"
        );

        self.gc(storage);

        let chunkwise_reductions = &mut self.chunkwise_reductions;
        let group_size = self.group_size;
        let rules = &self.rules;

        let idx = storage.internal_idx_of(chunk_key)?;
        let internal_storage = storage.internal_rvec()[idx].internal_rvec();

        chunkwise_reductions
            .entry(chunk_key.clone())
            .or_insert_with(|| Reduce::new(internal_storage, group_size, rules.clone()))
            .update(&internal_storage)
    }
}
