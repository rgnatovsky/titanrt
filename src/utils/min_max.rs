use rand::distributions::Uniform;
use rand::distributions::uniform::SampleUniform;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct MinMax<T> {
    pub min: T,
    pub max: T,
}

impl<T: PartialOrd + Copy + Ord> MinMax<T> {
    pub fn is_valid(&self) -> bool {
        self.min <= self.max
    }
    pub fn ordered(self) -> Self {
        if self.min <= self.max {
            self
        } else {
            Self {
                min: self.max,
                max: self.min,
            }
        }
    }

    pub fn contains(&self, x: T) -> bool {
        self.min <= x && x <= self.max
    }
    pub fn contains_exclusive(&self, x: T) -> bool {
        self.min < x && x < self.max
    }

    pub fn contains_range(&self, other: &Self) -> bool {
        self.min <= other.min && other.max <= self.max
    }
    pub fn intersects(&self, other: &Self) -> bool {
        self.min <= other.max && other.min <= self.max
    }

    pub fn clamp(&self, x: T) -> T {
        if x < self.min {
            self.min
        } else if x > self.max {
            self.max
        } else {
            x
        }
    }
    pub fn clamp_mut(&mut self, x: &mut T) {
        if *x < self.min {
            *x = self.min
        } else if *x > self.max {
            *x = self.max
        }
    }

    pub fn union(&self, other: &Self) -> Self {
        Self {
            min: self.min.min(other.min),
            max: self.max.max(other.max),
        }
    }
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        let lo = self.min.max(other.min);
        let hi = self.max.min(other.max);
        (lo <= hi).then_some(Self { min: lo, max: hi })
    }

    pub fn expand_to_include(&mut self, x: T) {
        if x < self.min {
            self.min = x;
        }
        if x > self.max {
            self.max = x;
        }
    }

    pub fn map<U: PartialOrd + Copy + Ord>(self, mut f: impl FnMut(T) -> U) -> MinMax<U> {
        // если f монотонно возрастает — просто применится; если нет, упорядочим
        let a = f(self.min);
        let b = f(self.max);
        MinMax {
            min: a.min(b),
            max: a.max(b),
        }
    }
}

impl<T> MinMax<T>
where
    T: PartialOrd + Copy + Add<Output = T> + Sub<Output = T> + Div<Output = T> + From<u8>,
{
    pub fn mid(&self) -> T {
        (self.min + self.max) / 2u8.into()
    }
    pub fn len(&self) -> T {
        self.max - self.min
    } // для usize можно сделать Option<T>
}

// Конструктор с проверкой (удобно, чтобы не забыть про min≤max)
impl<T: PartialOrd + Copy> TryFrom<(T, T)> for MinMax<T> {
    type Error = RangeError;
    fn try_from((min, max): (T, T)) -> Result<Self, Self::Error> {
        if min <= max {
            Ok(Self { min, max })
        } else {
            Err(RangeError::MinGreaterThanMax)
        }
    }
}

impl<T: PartialOrd + Copy> From<std::ops::RangeInclusive<T>> for MinMax<T> {
    fn from(r: std::ops::RangeInclusive<T>) -> Self {
        Self {
            min: *r.start(),
            max: *r.end(),
        }
    }
}
impl<T: PartialOrd + Copy> From<MinMax<T>> for std::ops::RangeInclusive<T> {
    fn from(r: MinMax<T>) -> Self {
        r.min..=r.max
    }
}

#[derive(Debug)]
pub enum RangeError {
    MinGreaterThanMax,
}
impl fmt::Display for RangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeError::MinGreaterThanMax => write!(f, "min > max"),
        }
    }
}
impl std::error::Error for RangeError {}

use std::ops::{Add, Div, Sub};

impl<T> MinMax<T>
where
    T: PartialOrd + Copy + SampleUniform,
{
    pub fn sample(&self, rng: &mut impl rand::Rng) -> T {
        rng.sample(Uniform::new_inclusive(self.min, self.max))
    }
}

impl<T: PartialOrd + Copy + Ord> MinMax<T> {
    pub fn new(min: T, max: T) -> Self {
        Self { min, max }.ordered()
    }
    pub fn try_new(min: T, max: T) -> Option<Self> {
        (min <= max).then_some(Self { min, max })
    }
}

impl MinMax<usize> {
    pub fn sample_n(&self, rng: &mut impl rand::Rng, n: usize) -> Vec<usize> {
        use rand::seq::index::sample;
        let len = self.max.saturating_sub(self.min) + 1;
        let n = n.min(len);
        let idx = sample(rng, len, n);
        idx.into_iter().map(|i| self.min + i).collect()
    }
}

impl MinMax<f64> {
    pub fn linspace(&self, points: usize) -> impl Iterator<Item = f64> + '_ {
        let n = points.max(2) - 1;
        let step = (self.max - self.min) / n as f64;
        (0..=n).map(move |i| self.min + step * i as f64)
    }
}
