include!("generated_parts/part_1.rs");
include!("generated_parts/part_2.rs");
include!("generated_parts/part_3.rs");
include!("generated_parts/part_4.rs");
include!("generated_parts/part_5.rs");

#[cfg(test)]
mod generated_tests {
    include!("generated_parts/tests_1.rs");
    include!("generated_parts/tests_2.rs");
    include!("generated_parts/tests_3.rs");
    include!("generated_parts/tests_4.rs");
}
