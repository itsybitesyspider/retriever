use crate::internal::mr::mrvec::MrVec;
use crate::internal::mr::reduce::*;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::storage::Storage;
use std::collections::HashMap;

/// Summarize a storage using a cached multi-pass reduction. Repeated evaluations will only re-compute
/// the parts of the storage that have changed.
pub struct Reduction<ChunkKey, Element, Summary> {
    parent_id: u64,
    group_size: usize,
    gc_chunk_list: MrVec<Option<ChunkKey>>,
    rules: ReduceRules<Element, Summary>,
    chunkwise_reductions:
        HashMap<ChunkKey, Reduce<Element, Summary>, crate::internal::hasher::HasherImpl>,
    chunkwise_summaries: MrVec<Summary>,
    reduction: Reduce<Summary, Summary>,
}

impl<ChunkKey, Element, Summary> Reduction<ChunkKey, Element, Summary>
where
    ChunkKey: ValidKey,
    Summary: Default + Clone,
{
    /// Create a new Reduction of a storage.
    ///
    /// Reduction::expensive_new(..) returns immediately. The first time it is used, however,
    /// each new Reduction will need to fully process every element of the storage it
    /// summarizes. Creating and dropping a lot of Reductions is therefore wasteful and pointless.
    ///
    /// Avoid calls to Reduction::expensive_new() by caching reductions as much as possible.
    pub fn new_expensive<I, E, Map, Fold>(
        storage: &Storage<ChunkKey, I, E>,
        group_size: usize,
        map: Map,
        fold: Fold,
    ) -> Self
    where
        I: ValidKey,
        E: Record<ChunkKey, I>,
        Map: Fn(&Element, &Summary) -> Option<Summary> + Clone + 'static,
        Fold: Fn(&[Summary], &Summary) -> Option<Summary> + Clone + 'static,
    {
        let chunkwise_summaries = MrVec::default();
        let reduction = Reduce::new(
            &chunkwise_summaries,
            group_size,
            Self::reduction_rules(map.clone(), fold.clone()),
        );
        Reduction {
            parent_id: storage.id(),
            group_size,
            gc_chunk_list: MrVec::default(),
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

    /// Summarize the given Storage using the rules provided for this Reduction.
    ///
    /// ```
    /// use retriever::*;
    /// use retriever::summaries::reduction::*;
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
    ///   Reduction::new_expensive(
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
    /// let summary = reduction.summarize(&storage).unwrap();
    ///
    /// assert_eq!(summary.count, 4);
    /// assert_eq!(summary.priority, 8);
    /// assert!(summary.services.contains("power"));
    /// assert!(summary.services.contains("mail"));
    /// assert!(summary.services.contains("games"));
    ///```
    pub fn summarize<ItemKey>(
        &mut self,
        storage: &Storage<ChunkKey, ItemKey, Element>,
    ) -> Option<&Summary>
    where
        Element: Record<ChunkKey, ItemKey>,
        ItemKey: ValidKey,
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

        chunkwise_summaries.map_reduce(&self.gc_chunk_list, 1, |chunk_key, _old_summary, idx| {
            assert!(chunk_key.len() <= 1);

            if chunk_key.is_empty() {
                return None;
            }

            let chunk_key = chunk_key[0]
                .as_ref()
                .cloned()
                .expect("retriever bug: chunk keys should be defined for all indices after gc");
            let internal_storage = storage.internal_mrvec()[idx].internal_mrvec();

            chunkwise_reductions
                .entry(chunk_key)
                .or_insert_with(|| Reduce::new(internal_storage, group_size, rules.clone()))
                .update(&internal_storage)
                .cloned()
        });

        self.reduction.update(&self.chunkwise_summaries)
    }
}
