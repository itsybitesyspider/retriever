use crate::internal::mr::rvec::RVec;
use crate::traits::memory_usage::MemoryUsage;
use crate::traits::memory_usage::MemoryUser;
use std::sync::Arc;

pub(crate) struct ReduceRules<Element, Summary> {
    map: Arc<dyn Fn(&Element, &Summary, usize) -> Option<Summary> + Send + Sync + 'static>,
    reduce: Arc<dyn Fn(&[Summary], &Summary) -> Option<Summary> + Send + Sync + 'static>,
}

pub(crate) struct Reduce<Element, Summary> {
    rules: ReduceRules<Element, Summary>,
    // This is a reduction stack. It looks like:
    // [[1], [..group_size], [..group_size^2], [..group_size^3], [..group_size^4], ..]
    // Where [..X] means "A vector of length X".
    // Used for exponential collapse the reduction vector into a single element.
    reductions: Vec<RVec<Summary>>,
    group_size: usize,
}

impl<Element, Summary> ReduceRules<Element, Summary> {
    pub(crate) fn new<Map, Reduce>(map: Map, reduce: Reduce) -> ReduceRules<Element, Summary>
    where
        Map: Fn(&Element, &Summary, usize) -> Option<Summary> + Send + Sync + 'static,
        Reduce: Fn(&[Summary], &Summary) -> Option<Summary> + Send + Sync + 'static,
    {
        ReduceRules {
            map: Arc::new(map),
            reduce: Arc::new(reduce),
        }
    }
}

impl<Element, Summary> Reduce<Element, Summary>
where
    Summary: Default,
{
    pub(crate) fn new(
        _parent: &RVec<Element>,
        group_size: usize,
        rules: ReduceRules<Element, Summary>,
    ) -> Self {
        assert!(group_size > 1);

        Reduce {
            rules,
            reductions: vec![RVec::default()],
            group_size,
        }
    }

    pub(crate) fn update(&mut self, parent: &RVec<Element>) -> Option<&Summary> {
        let mut layer = 0;
        let map = &self.rules.map;
        let reduce = &self.rules.reduce;

        if parent.len() == 0 {
            self.reductions[layer] = RVec::default();
        } else {
            self.reductions[layer].reduce(parent, 1, |xs, y, i| {
                if xs.is_empty() {
                    None
                } else {
                    (map)(&xs[0], y, i)
                }
            });
        }

        while self.reductions[layer].len() > 1 {
            if self.reductions.len() == layer + 1 {
                self.reductions.push(RVec::default());
            }

            let (left, right) = self.reductions.split_at_mut(layer + 1);
            right[0].reduce(&left[layer], self.group_size, |xs, y, _| {
                if xs.is_empty() {
                    None
                } else {
                    (reduce)(xs, y)
                }
            });

            layer += 1;
        }

        self.reductions.truncate(layer + 1);

        self.peek()
    }

    pub(crate) fn peek(&self) -> Option<&Summary> {
        let result_slice = &self.reductions[self.reductions.len() - 1];

        if result_slice.len() == 0 {
            None
        } else if result_slice.len() == 1 {
            Some(&result_slice[0])
        } else {
            panic!("bug in retriever");
        }
    }
}

impl<Element, Summary> Clone for ReduceRules<Element, Summary> {
    fn clone(&self) -> Self {
        ReduceRules {
            map: Arc::clone(&self.map),
            reduce: Arc::clone(&self.reduce),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn summation_rules() -> ReduceRules<i64, i64> {
        ReduceRules {
            map: Arc::new(
                |n: &i64, old_n: &i64, _: usize| {
                    if n != old_n {
                        Some(*n)
                    } else {
                        None
                    }
                },
            ),
            reduce: Arc::new(|ns: &[i64], _old_n: &i64| Some(ns.iter().cloned().sum::<i64>())),
        }
    }

    #[test]
    fn test_sum() {
        use super::*;

        let mut numbers = RVec::default();

        numbers.push(1);
        numbers.push(2);
        numbers.push(3);
        numbers.push(4);
        numbers.push(5);
        numbers.push(6);
        numbers.push(7);

        let mut sum = Reduce::new(&numbers, 2, summation_rules());

        sum.update(&numbers);

        assert_eq!(sum.peek(), Some(&28));
    }

    #[test]
    fn test_sum_with_update() {
        use super::*;

        let mut numbers = RVec::default();

        numbers.push(1);
        numbers.push(2);
        numbers.push(3);
        numbers.push(4);
        numbers.push(5);
        numbers.push(6);
        numbers.push(7);

        let mut sum = Reduce::new(&numbers, 2, summation_rules());

        sum.update(&numbers);
        assert_eq!(sum.peek(), Some(&28));

        numbers[3] += 10;

        sum.update(&numbers);
        assert_eq!(sum.peek(), Some(&38));
    }

    #[test]
    fn test_sum_with_removal() {
        use super::*;

        let mut numbers = RVec::default();

        numbers.push(1);
        numbers.push(2);
        numbers.push(3);
        numbers.push(4);
        numbers.push(5);
        numbers.push(6);
        numbers.push(7);

        let mut sum = Reduce::new(&numbers, 2, summation_rules());

        sum.update(&numbers);
        assert_eq!(sum.peek(), Some(&28));

        numbers.swap_remove(3);

        sum.update(&numbers);
        assert_eq!(sum.peek(), Some(&24));
    }

    #[test]
    fn test_sum_with_addition() {
        use super::*;

        let mut numbers = RVec::default();

        numbers.push(1);
        numbers.push(2);
        numbers.push(3);
        numbers.push(4);
        numbers.push(5);
        numbers.push(6);
        numbers.push(7);

        let mut sum = Reduce::new(&numbers, 2, summation_rules());

        sum.update(&numbers);
        assert_eq!(sum.peek(), Some(&28));

        numbers.push(8);

        sum.update(&numbers);
        assert_eq!(sum.peek(), Some(&36));
    }
}

impl<Element, Summary> MemoryUser for Reduce<Element, Summary> {
    fn memory_usage(&self) -> MemoryUsage {
        let mut result = self.reductions.memory_usage();

        for r in self.reductions.iter() {
            result = MemoryUsage::merge(result, r.memory_usage());
        }

        result
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        self.reductions.shrink_with(&f);

        for r in self.reductions.iter_mut() {
            r.shrink_with(&f);
        }
    }
}
