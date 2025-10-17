use std::{
    collections::HashMap,
    hash::Hash,
    ops::{Add, Div},
};

use rand::{
    Rng,
    distributions::WeightedIndex,
    prelude::Distribution,
    seq::{IteratorRandom, SliceRandom},
    thread_rng,
};

use crate::utils::floatings::round_f64;

pub struct SmartIter<'a, T> {
    items: &'a [T],
}

impl<'a, T> SmartIter<'a, T> {
    pub fn new(items: &'a [T]) -> Self {
        Self { items }
    }

    pub fn sample(&self) -> Option<&'a T> {
        let mut rng = thread_rng();
        self.items.choose(&mut rng)
    }

    pub fn sample_multiple(&self, k: usize) -> Vec<&'a T> {
        let mut rng = thread_rng();
        self.items.choose_multiple(&mut rng, k).collect()
    }

    pub fn shuffle(&self) -> Vec<&'a T> {
        let mut rng = thread_rng();
        let mut items: Vec<&T> = self.items.iter().collect();
        items.shuffle(&mut rng);
        items
    }

    pub fn sample_index(&self) -> Option<usize> {
        if self.items.is_empty() {
            None
        } else {
            let mut rng = thread_rng();
            Some(rng.gen_range(0..self.items.len()))
        }
    }

    pub fn sample_indices(&self, k: usize) -> Vec<usize> {
        let mut rng = thread_rng();
        rand::seq::index::sample(&mut rng, self.items.len(), k).into_vec()
    }

    pub fn sample_one_filtered<F>(&self, pred: F) -> Option<&'a T>
    where
        F: Fn(&T) -> bool,
    {
        let mut rng = thread_rng();
        self.items
            .iter()
            .filter(|&item| pred(item))
            .choose(&mut rng)
    }

    pub fn sample_k_filtered<F>(&self, k: usize, pred: F) -> Vec<&'a T>
    where
        F: Fn(&T) -> bool,
    {
        let mut rng = thread_rng();
        self.items
            .iter()
            .filter(|&item| pred(item))
            .choose_multiple(&mut rng, k)
    }

    pub fn weighted_sample_filtered<Fp, Fw>(&self, pass: Fp, weight_fn: Fw) -> Option<&'a T>
    where
        Fp: Fn(&T) -> bool,
        Fw: Fn(&T) -> f64,
    {
        let cand: Vec<&T> = self.items.iter().filter(|item| pass(item)).collect();
        if cand.is_empty() {
            return None;
        }

        let weights: Vec<f64> = cand.iter().map(|item| weight_fn(item)).collect();
        if weights.iter().all(|&w| w <= 0.0) {
            return None;
        }

        let dist = WeightedIndex::new(&weights).ok()?;
        let mut rng = thread_rng();
        Some(cand[dist.sample(&mut rng)])
    }

    pub fn weighted_sample<Fw>(&self, weight_fn: Fw) -> Option<&'a T>
    where
        Fw: Fn(&T) -> f64,
    {
        if self.items.is_empty() {
            return None;
        }

        let weights: Vec<f64> = self.items.iter().map(|item| weight_fn(item)).collect();
        if weights.iter().all(|&w| w <= 0.0) {
            return None;
        }

        let dist = WeightedIndex::new(&weights).ok()?;
        let mut rng = thread_rng();
        Some(&self.items[dist.sample(&mut rng)])
    }

    pub fn weighted_sample_multiple<Fw>(&self, k: usize, weight_fn: Fw) -> Vec<&'a T>
    where
        Fw: Fn(&T) -> f64,
    {
        let mut result = Vec::with_capacity(k);
        for _ in 0..k {
            if let Some(item) = self.weighted_sample(&weight_fn) {
                result.push(item);
            }
        }
        result
    }

    pub fn partition<F>(&self, pred: F) -> (Vec<&'a T>, Vec<&'a T>)
    where
        F: Fn(&T) -> bool,
    {
        self.items.iter().partition(|&item| pred(item))
    }

    pub fn find<F>(&self, pred: F) -> Option<&'a T>
    where
        F: Fn(&T) -> bool,
    {
        self.items.iter().find(|&item| pred(item))
    }

    pub fn filter<F>(&self, pred: F) -> Vec<&'a T>
    where
        F: Fn(&T) -> bool,
    {
        self.items.iter().filter(|&item| pred(item)).collect()
    }

    pub fn count_if<F>(&self, pred: F) -> usize
    where
        F: Fn(&T) -> bool,
    {
        self.items.iter().filter(|&item| pred(item)).count()
    }

    pub fn any<F>(&self, pred: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        self.items.iter().any(|item| pred(item))
    }

    pub fn all<F>(&self, pred: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        self.items.iter().all(|item| pred(item))
    }

    pub fn sample_with_probability(&self, probability: f64) -> Option<&'a T> {
        let mut rng = thread_rng();
        if rng.r#gen::<f64>() < probability {
            self.sample()
        } else {
            None
        }
    }

    /// Выбрать K элементов, каждый с вероятностью p (Bernoulli sampling)
    pub fn bernoulli_sample(&self, probability: f64) -> Vec<&'a T> {
        let mut rng = thread_rng();
        self.items
            .iter()
            .filter(|_| rng.r#gen::<f64>() < probability)
            .collect()
    }
}

impl<'a, T> SmartIter<'a, T>
where
    T: PartialOrd + Copy,
{
    pub fn top_k(&self, k: usize) -> Vec<&'a T> {
        let mut sorted: Vec<&T> = self.items.iter().collect();
        sorted.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        sorted.into_iter().take(k).collect()
    }

    pub fn bottom_k(&self, k: usize) -> Vec<&'a T> {
        let mut sorted: Vec<&T> = self.items.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted.into_iter().take(k).collect()
    }
}

impl<'a, T> SmartIter<'a, T>
where
    T: PartialOrd + Copy + Ord,
{
    pub fn min(&self) -> Option<&'a T> {
        self.items.iter().min()
    }

    pub fn max(&self) -> Option<&'a T> {
        self.items.iter().max()
    }
}

impl<'a, T> SmartIter<'a, T>
where
    T: Add<Output = T> + Copy + Default,
{
    /// Сумма всех элементов
    pub fn sum(&self) -> T {
        self.items
            .iter()
            .copied()
            .fold(T::default(), |acc, x| acc + x)
    }
}

impl<'a, T> SmartIter<'a, T>
where
    T: Add<Output = T> + Div<Output = T> + Copy + Default + From<usize>,
{
    /// Среднее значение
    pub fn mean(&self) -> Option<T> {
        if self.items.is_empty() {
            None
        } else {
            let sum = self.sum();
            Some(sum / T::from(self.items.len()))
        }
    }
}

impl SmartIter<'_, f64> {
    pub fn sum_with_precision(&self, precision: i32) -> f64 {
        self.items
            .iter()
            .copied()
            .fold(0.0, |acc, x| round_f64(acc + x, precision))
    }

    pub fn mean_std(&self) -> Option<(f64, f64)> {
        if self.items.is_empty() {
            return None;
        }

        let mean = self.items.iter().sum::<f64>() / self.items.len() as f64;
        let variance =
            self.items.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / self.items.len() as f64;

        Some((mean, variance.sqrt()))
    }

    pub fn median(&self) -> Option<f64> {
        if self.items.is_empty() {
            return None;
        }

        let mut sorted = self.items.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mid = sorted.len() / 2;
        if sorted.len() % 2 == 0 {
            Some((sorted[mid - 1] + sorted[mid]) / 2.0)
        } else {
            Some(sorted[mid])
        }
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.items.is_empty() || q < 0.0 || q > 1.0 {
            return None;
        }

        let mut sorted = self.items.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let index = (q * (sorted.len() - 1) as f64).round() as usize;
        Some(sorted[index])
    }
}

impl<'a, T> SmartIter<'a, T> {
    pub fn group_by<K, F>(&self, key_fn: F) -> HashMap<K, Vec<&'a T>>
    where
        K: Eq + Hash,
        F: Fn(&T) -> K,
    {
        let mut groups: HashMap<K, Vec<&T>> = HashMap::new();
        for item in self.items.iter() {
            let key = key_fn(item);
            groups.entry(key).or_default().push(item);
        }
        groups
    }

    pub fn sorted(&self) -> Vec<&'a T>
    where
        T: Ord,
    {
        let mut sorted: Vec<&T> = self.items.iter().collect();
        sorted.sort();
        sorted
    }

    pub fn sorted_by<F>(&self, compare: F) -> Vec<&'a T>
    where
        F: FnMut(&&T, &&T) -> std::cmp::Ordering,
    {
        let mut sorted: Vec<&T> = self.items.iter().collect();
        sorted.sort_by(compare);
        sorted
    }

    pub fn sorted_by_key<K, F>(&self, key_fn: F) -> Vec<&'a T>
    where
        K: Ord,
        F: FnMut(&&T) -> K,
    {
        let mut sorted: Vec<&T> = self.items.iter().collect();
        sorted.sort_by_key(key_fn);
        sorted
    }

    pub fn unique(&self) -> Vec<&'a T>
    where
        T: Eq + Hash,
    {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        self.items
            .iter()
            .filter(|item| seen.insert(*item))
            .collect()
    }

    pub fn dedup(&self) -> Vec<&'a T>
    where
        T: PartialEq,
    {
        let mut result = Vec::new();
        let mut last: Option<&T> = None;

        for item in self.items.iter() {
            if last != Some(item) {
                result.push(item);
                last = Some(item);
            }
        }

        result
    }
}

impl<'a, T> SmartIter<'a, T> {
    /// Количество элементов
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Пустая ли коллекция
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Получить все элементы
    pub fn items(&self) -> &[T] {
        self.items
    }

    /// Итератор по элементам
    pub fn iter(&self) -> impl Iterator<Item = &'a T> {
        self.items.iter()
    }
    pub fn chain<'b>(&'b self, other: &'b SmartIter<'a, T>) -> impl Iterator<Item = &'a T> + 'b {
        self.items.iter().chain(other.items.iter())
    }

    /// Zip с другой коллекцией
    pub fn zip<U>(&'a self, other: &'a [U]) -> impl Iterator<Item = (&'a T, &'a U)> {
        self.items.iter().zip(other.iter())
    }

    /// Enumerate: с индексами
    pub fn enumerate(&self) -> impl Iterator<Item = (usize, &T)> {
        self.items.iter().enumerate()
    }

    /// Take: первые N элементов
    pub fn take(&self, n: usize) -> Vec<&'a T> {
        self.items.iter().take(n).collect()
    }

    /// Skip: пропустить первые N
    pub fn skip(&self, n: usize) -> Vec<&'a T> {
        self.items.iter().skip(n).collect()
    }

    /// Chunks: разбить на куски
    pub fn chunks(&self, size: usize) -> impl Iterator<Item = &[T]> {
        self.items.chunks(size)
    }

    /// Windows: скользящее окно
    pub fn windows(&self, size: usize) -> impl Iterator<Item = &[T]> {
        self.items.windows(size)
    }
}

impl<'a, T> IntoIterator for SmartIter<'a, T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl<'a, 'b, T> IntoIterator for &'b SmartIter<'a, T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl<'a, T> SmartIter<'a, T> {
    pub fn collect<B>(&self) -> B
    where
        B: FromIterator<&'a T>,
    {
        self.items.iter().collect()
    }

    pub fn to_vec(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.items.to_vec()
    }

    pub fn to_set(&self) -> std::collections::HashSet<&'a T>
    where
        T: Eq + Hash,
    {
        self.items.iter().collect()
    }

    pub fn select(&self, indices: &[usize]) -> Vec<&'a T> {
        indices.iter().filter_map(|&i| self.items.get(i)).collect()
    }

    pub fn pluck<U, F>(&self, f: F) -> Vec<U>
    where
        F: Fn(&T) -> U,
    {
        self.items.iter().map(f).collect()
    }
}
