// Utility macro to perform conditional early returns
#[macro_export]
macro_rules! check {
    ($cond:expr, $rst:expr) => {
        if $cond {
            return $rst;
        }
    };
}

// Utility macro to handle conditional skips with different patterns
#[macro_export]
macro_rules! skip {
    // Pattern for expression and block
    ($condition:expr, $block:block) => {
        if $condition {
            $block
            continue;
        }
    };

    // Pattern for just expression
    ($condition:expr, $expr:expr) => {
        if $condition {
            $expr;
            continue;
        }
    };

    // Pattern for condition only
    ($condition:expr) => {
        if $condition {
            continue;
        }
    };
}

#[macro_export]
macro_rules! impl_traits {
    ($type:ty, $($trait:path),*) => {
        $(
            impl $trait for $type {}
        )*
    };
}