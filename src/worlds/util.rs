
#[derive(Clone, Debug)]
pub struct Record {
    pub key: u64,
    pub value: f64,
}

//--------------AggState--------------//
#[derive(Clone, Debug)]
pub enum AggState {
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: usize },
    Count(f64),
}

impl AggState {
    // Create an initial state from a value and aggregation type
    pub fn new_from_agg(agg: &str, value: f64) -> Self {
        match agg {
            "sum" => AggState::Sum(value),
            "min" => AggState::Min(value),
            "max" => AggState::Max(value),
            "avg" => AggState::Avg { sum: value, count: 1 },
            "count" => AggState::Count(1.0),
            _ => panic!("unsupported aggregation: {}", agg),
        }
    }

    // Update the running aggregate with a new record value
    pub fn update(&mut self, value: f64) {
        match self {
            AggState::Sum(v) => *v += value,
            AggState::Min(v) => *v = v.min(value),
            AggState::Max(v) => *v = v.max(value),
            AggState::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
            AggState::Count(v) => *v += 1.0,
        }
    }

    // Merge another AggState of the same kind
    pub fn merge(&mut self, other: AggState) {
        match (self, other) {
            (AggState::Sum(a), AggState::Sum(b)) => *a += b,
            (AggState::Min(a), AggState::Min(b)) => *a = a.min(b),
            (AggState::Max(a), AggState::Max(b)) => *a = a.max(b),
            (
                AggState::Avg { sum: s1, count: c1 },
                AggState::Avg { sum: s2, count: c2 },
            ) => {
                *s1 += s2;
                *c1 += c2;
            },
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            _ => panic!("Mismatched aggregate types during merge"),
        }
    }

    // Compute the final output value
    pub fn finalize(&self) -> f64 {
        match self {
            AggState::Sum(v) => *v,
            AggState::Min(v) => *v,
            AggState::Max(v) => *v,
            AggState::Avg { sum, count } => *sum / *count as f64,
            AggState::Count(v) => *v,
        }
    }
}