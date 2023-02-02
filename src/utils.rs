pub use meanstd::*;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::*;

pub fn setup_logger(debug: bool) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(Some(tracing_subscriber::fmt::layer().with_filter(
            if debug {
                LevelFilter::DEBUG
            } else {
                LevelFilter::INFO
            },
        )))
        .init();

    Ok(())
}
pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub mod meanstd {
    use serde::{Deserialize, Serialize};

    static DEFAULT_DISPLAY_PRECISION: usize = 3;

    #[derive(Serialize, Deserialize, Clone)]
    pub struct MeanStd {
        pub mean: f64,
        pub std: f64,
        pub min: f64,
        pub max: f64,
        pub count: usize,
    }
    impl Default for MeanStd {
        fn default() -> Self {
            Self {
                mean: f64::NAN,
                std: f64::NAN,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                count: 0,
            }
        }
    }
    impl MeanStd {
        pub fn is_empty(&self) -> bool {
            self.count == 0
        }
        pub fn range(&self, precision: usize) -> String {
            format!("{:.*} - {:.*}", precision, self.min, precision, self.max)
        }
    }

    impl std::fmt::Display for MeanStd {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            if self.is_empty() {
                write!(f, "-")
            } else {
                let prec = f.precision().unwrap_or(DEFAULT_DISPLAY_PRECISION);
                write!(f, "{:.*} ± {:.*}", prec, self.mean, prec, self.std)
            }
        }
    }
    impl std::fmt::Debug for MeanStd {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self)
        }
    }
    impl std::ops::Mul<&MeanStd> for f64 {
        type Output = MeanStd;

        fn mul(self, rhs: &MeanStd) -> Self::Output {
            let minmax = [self * rhs.min, self * rhs.max];
            MeanStd {
                mean: self * rhs.mean,
                std: self * self * rhs.std,
                min: minmax[0].min(minmax[1]),
                max: minmax[0].max(minmax[1]),
                count: rhs.count,
            }
        }
    }
    macro_rules! meanstd_from_it {
        ($t:ty,$filter:expr) => {
            impl FromIterator<$t> for MeanStd {
                fn from_iter<T>(it: T) -> MeanStd
                where
                    T: IntoIterator<Item = $t>,
                {
                    // Welford's algorithm
                    let mut max: f64 = f64::NEG_INFINITY;
                    let mut min: f64 = f64::INFINITY;
                    let mut mean: f64 = 0.0;
                    let mut m: f64 = 0.0;
                    let mut i: f64 = 0.0;
                    for x in it.into_iter().filter($filter) {
                        let x = x as f64;
                        max = max.max(x);
                        min = min.min(x);
                        i += 1.0;
                        let d = x - mean;
                        mean += d / i;
                        m += d * (x - mean);
                    }
                    if i == 0.0 {
                        return Default::default();
                    }
                    MeanStd {
                        mean,
                        min,
                        max,
                        std: (m / i).sqrt(),
                        count: i as usize,
                    }
                }
            }
        };
    }
    meanstd_from_it!(f32, |x| !x.is_nan());
    meanstd_from_it!(f64, |x| !x.is_nan());
    meanstd_from_it!(u64, |_| true);
}
