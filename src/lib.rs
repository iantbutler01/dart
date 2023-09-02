#![feature(portable_simd)]

mod errors;
mod locking;
mod node;
pub mod range;
pub mod tree;
mod utils;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2, 2);
    }
}
