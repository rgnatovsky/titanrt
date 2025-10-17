use std::ops::{Add, Div};

use rand::{
    Rng,
    distributions::WeightedIndex,
    prelude::Distribution,
    seq::{IteratorRandom, SliceRandom},
    thread_rng,
};

use crate::utils::floatings::round_f64;

pub struct Sampler<'a, T> {
    items: &'a [T],
}

impl<'a, T> Sampler<'a, T> {
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

impl<'a, T> Sampler<'a, T>
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

impl<'a, T> Sampler<'a, T>
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

impl<'a, T> Sampler<'a, T>
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

impl<'a, T> Sampler<'a, T>
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

impl Sampler<'_, f64> {
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

impl<'a, T> Sampler<'a, T> {
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
}
