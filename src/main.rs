mod worlds;

use worlds::medium::groupby_agg as groupby_agg_medium;
use worlds::large::groupby_agg as groupby_agg_large;
use worlds::util::{Record, GroupByResult};


fn main() {
    let records = vec![
        Record { key: 1, value: 10.0 },
        Record { key: 2, value: 20.0 },
        Record { key: 1, value: 5.0 },
        Record { key: 3, value: 7.5 },
        Record { key: 2, value: 2.5 },
        Record { key: 1, value: 4.0 },
        Record { key: 1, value: 10.0 },
        Record { key: 2, value: 20.0 },
        Record { key: 1, value: 5.0 },
        Record { key: 3, value: 7.5 },
        Record { key: 2, value: 2.5 },
        Record { key: 1, value: 4.0 },
    ];

    println!("Running Medium world...");
    let groups = groupby_agg_medium(&records, "count");
    let result_medium = GroupByResult { raw: groups };
    println!("Result: {:?}", result_medium.raw);
    println!("Finalized = {:?}", result_medium.finalize());

    println!("\nRunning Large world...");
    let result_large = groupby_agg_large(&records, "sum");
    println!("Result: {:?}", result_large);
}