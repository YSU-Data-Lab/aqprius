use rand::prelude::*;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use std::time::Instant;
/*creating a resampling function for generic datatype.
This function takes a reference to a vector of generic datatype and returns a vector of generic datatype.
*/
pub fn random_sample_with_replacement(sample: &[i64], size: usize) -> Vec<i64> {
    let resampled: Vec<i64> = (0..size)
        .into_par_iter()
        .map(|_| {
            let mut rng = thread_rng();
            *sample.choose(&mut rng).unwrap()
        })
        .collect();

    resampled
}

// //generating bootstrapping sample groundtruth using simple random sampling with replacement
pub fn bootstrap_sums(data: &[i64], num_resamples: usize, sample_fraction: f64) -> (Vec<i64>, f64) {
    let start_time = Instant::now();
    let bootstrap_sums: Vec<i64> = (0..num_resamples)
        .into_par_iter()
        .map(|_| {
            let resampled_data = random_sample_with_replacement(&data, data.len());
            let sum: i64 = resampled_data.iter().sum();
            (sum as f64 / sample_fraction) as i64
        })
        .collect();

    let elapsed_time = start_time.elapsed().as_secs_f64();

    (bootstrap_sums, elapsed_time)
}

//calculating mean of bootstrapping ground truth sample
pub fn calculate_mean(bootstrap_sums: &[i64], bootstrap_size: usize) -> f64 {
    let sum: i64 = bootstrap_sums.par_iter().sum();
    sum as f64 / bootstrap_size as f64
}

//calculating standard deviation of bootstrapping ground truth sample
pub fn calculate_variance(bootstrap_sums: &[i64], bootstrap_size: usize) -> f64 {
    let mean = calculate_mean(bootstrap_sums, bootstrap_size);
    let variance: f64 = bootstrap_sums
        .par_iter()
        .map(|&value| {
            let diff = value as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / bootstrap_size as f64
        - 1.0;

    variance.sqrt()
}
