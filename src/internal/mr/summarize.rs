use crate::internal::mr::rvec::RVec;
use crate::traits::memory_usage::{MemoryUsage, MemoryUser};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct SummaryRules<Element, Token, Summary> {
    pub(crate) map: Arc<dyn Fn(&Element, &Token, usize) -> Option<Token> + Send + Sync + 'static>,
    pub(crate) contribute: Arc<dyn Fn(&Token, usize, &mut Summary) + Send + Sync + 'static>,
    pub(crate) uncontribute: Arc<dyn Fn(&Token, usize, &mut Summary) + Send + Sync + 'static>,
}

/// Maintain a summary of an RVec by mutating a summary based on some token.
pub(crate) struct Summarize<Element, Token, Summary> {
    rules: Arc<SummaryRules<Element, Token, Summary>>,
    tokens: RVec<Token>,
    summary: Summary,
}

impl<Element, Token, Summary> Summarize<Element, Token, Summary>
where
    Token: Default + Eq,
{
    pub(crate) fn new(
        _source: &RVec<Element>,
        rules: Arc<SummaryRules<Element, Token, Summary>>,
    ) -> Self
    where
        Summary: Default,
    {
        Summarize {
            rules,
            tokens: RVec::default(),
            summary: Summary::default(),
        }
    }

    pub(crate) fn update(&mut self, parent: &RVec<Element>) {
        let tokens = &mut self.tokens;
        let map = &self.rules.map;
        let contribute = &self.rules.contribute;
        let uncontribute = &self.rules.uncontribute;
        let summary = &mut self.summary;

        tokens.reduce(parent, 1, move |elements, old_token, i| {
            if elements.is_empty() {
                if old_token != &Token::default() {
                    (uncontribute)(old_token, i, summary);
                }

                return None;
            }

            let result = (map)(&elements[0], old_token, i);

            if let Some(ref new_token) = result {
                if old_token != &Token::default() {
                    (uncontribute)(old_token, i, summary);
                }

                if new_token != &Token::default() {
                    (contribute)(new_token, i, summary);
                }
            }

            result
        });
    }

    pub(crate) fn peek(&self) -> &Summary {
        &self.summary
    }
}

impl<Element, Token, Summary> MemoryUser for Summarize<Element, Token, Summary>
where
    Token: Default + Eq,
{
    fn memory_usage(&self) -> MemoryUsage {
        self.tokens.memory_usage()
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        self.tokens.shrink_with(f);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn summation_rules() -> SummaryRules<i64, i64, i64> {
        SummaryRules {
            map: Arc::new(
                |n: &i64, old_n: &i64, _: usize| {
                    if n != old_n {
                        Some(*n)
                    } else {
                        None
                    }
                },
            ),
            contribute: Arc::new(|new_n: &i64, _: usize, summary: &mut i64| {
                *summary += new_n;
            }),
            uncontribute: Arc::new(|old_n: &i64, _: usize, summary: &mut i64| {
                *summary -= old_n;
            }),
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

        let mut sum = Summarize::new(&numbers, Arc::new(summation_rules()));

        sum.update(&numbers);

        assert_eq!(*sum.peek(), 28);
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

        let mut sum = Summarize::new(&numbers, Arc::new(summation_rules()));

        sum.update(&numbers);
        assert_eq!(*sum.peek(), 28);

        numbers[3] += 10;

        sum.update(&numbers);
        assert_eq!(*sum.peek(), 38);
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

        let mut sum = Summarize::new(&numbers, Arc::new(summation_rules()));

        sum.update(&numbers);
        assert_eq!(*sum.peek(), 28);

        numbers.swap_remove(3);

        sum.update(&numbers);
        assert_eq!(*sum.peek(), 24);
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

        let mut sum = Summarize::new(&numbers, Arc::new(summation_rules()));

        sum.update(&numbers);
        assert_eq!(*sum.peek(), 28);

        numbers.push(8);

        sum.update(&numbers);
        assert_eq!(*sum.peek(), 36);
    }
}
